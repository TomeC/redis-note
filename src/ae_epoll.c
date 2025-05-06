/* Linux epoll(2) based ae.c module
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <sys/epoll.h>
#include <sys/time.h>
#include "ae.h"
// 保存epoll额外产生的数据
typedef struct aeApiState
{
    int epfd;
    struct epoll_event *events;
} aeApiState;
// 调用epoll_create并保存返回值
static int aeApiCreate(aeEventLoop *eventLoop)
{
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state)
    {
        return -1;
    }
    state->events = zmalloc(sizeof(struct epoll_event) * eventLoop->setsize);
    if (!state->events)
    {
        zfree(state);
        return -1;
    }
    // 1024 只是一个对内核的建议
    state->epfd = epoll_create(1024);
    if (state->epfd == -1)
    {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    eventLoop->apidata = state;
    return 0;
}
// 重新设置事件的大小
static int aeApiResize(aeEventLoop *eventLoop, int setsize)
{
    aeApiState *state = eventLoop->apidata;

    state->events = zrealloc(state->events, sizeof(struct epoll_event) * setsize);
    return 0;
}
// 关闭连接
static void aeApiFree(aeEventLoop *eventLoop)
{
    aeApiState *state = eventLoop->apidata;

    close(state->epfd);
    zfree(state->events);
    zfree(state);
}
// 添加事件
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0};
    // 如果fd已经在使用了，那么修改
    int op = eventLoop->events[fd].mask == AE_NONE ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;

    ee.events = 0;
    mask |= eventLoop->events[fd].mask;
    if (mask & AE_READABLE)
    {
        ee.events |= EPOLLIN;
    }
    if (mask & AE_WRITABLE)
    {
        ee.events |= EPOLLOUT;
    }
    ee.data.fd = fd;
    if (epoll_ctl(state->epfd, op, fd, &ee) == -1)
    {
        return -1;
    }
    return 0;
}
// 删除epoll事件
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask)
{
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0};
    // 假如fd的mask是0001，要删除0001，那么mask=0001&(1110)就是0000
    int mask = eventLoop->events[fd].mask & (~delmask);

    ee.events = 0;
    if (mask & AE_READABLE)
    {
        ee.events |= EPOLLIN;
    }
    if (mask & AE_WRITABLE)
    {
        ee.events |= EPOLLOUT;
    }
    ee.data.fd = fd;
    if (mask != AE_NONE)
    {
        epoll_ctl(state->epfd, EPOLL_CTL_MOD, fd, &ee);
    }
    else
    {
        /* Note, Kernel < 2.6.9 requires a non null event pointer even for
         * EPOLL_CTL_DEL. */
        epoll_ctl(state->epfd, EPOLL_CTL_DEL, fd, &ee);
    }
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp)
{
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;
    // epfd：由 epoll_create 创建的 epoll 实例的文件描述符
    // events：用于存储返回事件的数组，每个元素是一个 epoll_event 结构体。
    // maxevents：最多返回的事件数量（不能超过初始化时分配的大小）。
    // timeout：超时时间（单位为毫秒），若为 -1 表示无限等待，0 表示立即返回。
    retval = epoll_wait(state->epfd, state->events, eventLoop->setsize,
                        tvp ? (tvp->tv_sec * 1000 + tvp->tv_usec / 1000) : -1);
    if (retval > 0)
    {
        int j;

        numevents = retval;
        for (j = 0; j < numevents; j++)
        {
            int mask = 0;
            struct epoll_event *e = state->events + j;

            if (e->events & EPOLLIN)
            {
                mask |= AE_READABLE;
            }
            if (e->events & EPOLLOUT)
            {
                mask |= AE_WRITABLE;
            }
            if (e->events & EPOLLERR)
            {
                mask |= AE_WRITABLE;
            }
            if (e->events & EPOLLHUP)
            {
                mask |= AE_WRITABLE;
            }
            eventLoop->fired[j].fd = e->data.fd;
            eventLoop->fired[j].mask = mask;
        }
    }
    return numevents;
}

static char *aeApiName(void)
{
    return "epoll";
}
