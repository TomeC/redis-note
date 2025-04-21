/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "ae.h"
#include "zmalloc.h"
#include "config.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
#ifdef HAVE_EPOLL
#include "ae_epoll.c"
#else
#ifdef HAVE_KQUEUE
#include "ae_kqueue.c"
#else
#include "ae_select.c"
#endif
#endif
#endif

aeEventLoop *aeCreateEventLoop(int setsize)
{
    aeEventLoop *eventLoop;
    int i;

    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL)
    {
        goto err;
    }
    eventLoop->events = zmalloc(sizeof(aeFileEvent) * setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent) * setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL)
    {
        goto err;
    }
    eventLoop->setsize = setsize;
    eventLoop->lastTime = time(NULL);
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    if (aeApiCreate(eventLoop) == -1)
    {
        goto err;
    }
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    for (i = 0; i < setsize; i++)
    {
        eventLoop->events[i].mask = AE_NONE;
    }
    return eventLoop;

err:
    if (eventLoop)
    {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size. */
int aeGetSetSize(aeEventLoop *eventLoop)
{
    return eventLoop->setsize;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful. */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize)
{
    int i;

    if (setsize == eventLoop->setsize)
        return AE_OK;
    if (eventLoop->maxfd >= setsize)
        return AE_ERR;
    if (aeApiResize(eventLoop, setsize) == -1)
        return AE_ERR;

    eventLoop->events = zrealloc(eventLoop->events, sizeof(aeFileEvent) * setsize);
    eventLoop->fired = zrealloc(eventLoop->fired, sizeof(aeFiredEvent) * setsize);
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    for (i = eventLoop->maxfd + 1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

void aeDeleteEventLoop(aeEventLoop *eventLoop)
{
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    zfree(eventLoop);
}

void aeStop(aeEventLoop *eventLoop)
{
    eventLoop->stop = 1;
}
// 事件注册函数
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                      aeFileProc *proc, void *clientData)
{
    if (fd >= eventLoop->setsize)
    {
        errno = ERANGE;
        return AE_ERR;
    }
    aeFileEvent *fe = &eventLoop->events[fd];

    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
    {
        return AE_ERR;
    }
    fe->mask |= mask;
    if (mask & AE_READABLE)
    {
        fe->rfileProc = proc;
    }
    if (mask & AE_WRITABLE)
    {
        fe->wfileProc = proc;
    }
    fe->clientData = clientData;
    if (fd > eventLoop->maxfd)
    {
        eventLoop->maxfd = fd;
    }
    return AE_OK;
}

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    if (fd >= eventLoop->setsize)
        return;
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE)
        return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed. */
    if (mask & AE_WRITABLE)
        mask |= AE_BARRIER;

    aeApiDelEvent(eventLoop, fd, mask);
    fe->mask = fe->mask & (~mask);
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE)
    {
        /* Update the max fd */
        int j;

        for (j = eventLoop->maxfd - 1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE)
                break;
        eventLoop->maxfd = j;
    }
}

int aeGetFileEvents(aeEventLoop *eventLoop, int fd)
{
    if (fd >= eventLoop->setsize)
        return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

static void aeGetTime(long *seconds, long *milliseconds)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec / 1000;
}

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms)
{
    long cur_sec, cur_ms, when_sec, when_ms;

    aeGetTime(&cur_sec, &cur_ms);
    when_sec = cur_sec + milliseconds / 1000;
    when_ms = cur_ms + milliseconds % 1000;
    if (when_ms >= 1000)
    {
        when_sec++;
        when_ms -= 1000;
    }
    *sec = when_sec;
    *ms = when_ms;
}

long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds, aeTimeProc *proc,
                            void *clientData, aeEventFinalizerProc *finalizerProc)
{
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));
    if (te == NULL)
    {
        return AE_ERR;
    }
    te->id = id;
    aeAddMillisecondsToNow(milliseconds, &te->when_sec, &te->when_ms);
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->prev = NULL;
    te->next = eventLoop->timeEventHead;
    if (te->next)
    {
        te->next->prev = te;
    }
    eventLoop->timeEventHead = te;
    return id;
}

int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    while (te)
    {
        if (te->id == id)
        {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

// O(N) 查找最近的定时任务
// todo 优化：有序插入；使用skipList优化查找和插入
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest = NULL;
    int eCount = 0;
    while (te)
    {
        // todo nearest==null时nearest->when_sec nullException ??
        if (!nearest || te->when_sec < nearest->when_sec ||
            (te->when_sec == nearest->when_sec && te->when_ms < nearest->when_ms))
        {
            nearest = te;
        }
        te = te->next;
        eCount++;
    }
    (void)eCount;
    return nearest;
}

static int processTimeEvents(aeEventLoop *eventLoop)
{
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;
    time_t now = time(NULL);

    /* If the system clock is moved to the future, and then set back to the
     * right value, time events may be delayed in a random way. Often this
     * means that scheduled operations will not be performed soon enough.
     *
     * Here we try to detect system clock skews, and force all the time
     * events to be processed ASAP when this happens: the idea is that
     * processing events earlier is less dangerous than delaying them
     * indefinitely, and practice suggests it is. */
    if (now < eventLoop->lastTime)
    { // 修改时间往后调了
        te = eventLoop->timeEventHead;
        while (te)
        {
            te->when_sec = 0;
            te = te->next;
        }
    }
    eventLoop->lastTime = now;

    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId - 1;
    while (te)
    {
        long now_sec, now_ms;
        long long id;

        /* Remove events scheduled for deletion. */
        if (te->id == AE_DELETED_EVENT_ID)
        {
            aeTimeEvent *next = te->next;
            if (te->prev)
            {
                te->prev->next = te->next;
            }
            else
            {
                eventLoop->timeEventHead = te->next;
            }
            if (te->next)
            {
                te->next->prev = te->prev;
            }
            if (te->finalizerProc)
            {
                te->finalizerProc(eventLoop, te->clientData);
            }
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense. 防御性编程*/
        if (te->id > maxId)
        {
            te = te->next;
            continue;
        }
        aeGetTime(&now_sec, &now_ms);
        // 触发时间到了
        if (now_sec > te->when_sec ||
            (now_sec == te->when_sec && now_ms >= te->when_ms))
        {
            int retval;

            id = te->id;
            retval = te->timeProc(eventLoop, id, te->clientData);
            processed++;
            if (retval != AE_NOMORE)
            {
                // 如果需要继续执行，设置下次触发时间
                aeAddMillisecondsToNow(retval, &te->when_sec, &te->when_ms);
            }
            else
            {
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        te = te->next;
    }
    return processed;
}

// 处理每个挂起的时间事件，然后处理每个挂起的文件事件（可能由刚刚处理的时间事件回调注册）。
//  如果没有特殊标志，函数将一直休眠，直到触发某个文件事件，或者下次事件发生时（如果有）
// 如果标志为0，那么函数不做任何事情直接返回
// 如果标志设置为AE_ALL_EVENTS，那么所有的事件都会被处理
// 如果标志设置为AE_FILE_EVENTS，那么文件事件会被处理
// 如果标志设置为AE_TIME_EVENTS，那么时间事件会被处理
// 如果标志设置了AE_DONT_WAIT，则函数将尽快返回，直到所有可以不等待处理的事件都处理完毕。
// 如果标志设置为AE_CALL_AFTER_SLEEP，那么aftersleep回调将会被调用
// 如果标志设置为AE_CALL_BEFORE_SLEEP，那么beforesleep回调将会被调用

int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;

    /* 若没有事件处理，则立刻返回 没看见调用的地方*/
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS))
    {
        return 0;
    }

    // 如果有IO事件或者非AE_DONT_WAIT得时间事件需要处理,processEventsWhileBlocked会设置AE_DONT_WAIT
    if (eventLoop->maxfd != -1 || ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT)))
    {
        int j;
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
        {
            // 查找最近的一个时间事件
            shortest = aeSearchNearestTimer(eventLoop);
        }
        if (shortest)
        {
            long now_sec, now_ms;

            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;

            // 需要等待的时间
            long long ms = (shortest->when_sec - now_sec) * 1000 + shortest->when_ms - now_ms;
            if (ms > 0)
            {
                tvp->tv_sec = ms / 1000;
                tvp->tv_usec = (ms % 1000) * 1000;
            }
            else
            {
                tvp->tv_sec = 0;
                tvp->tv_usec = 0;
            }
        }
        else
        {
            // 不等待，立刻返回
            if (flags & AE_DONT_WAIT)
            {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            }
            else
            {
                // 等待直到有事件被触发
                tvp = NULL;
            }
        }

        // 等待超时或者有事件触发 epoll_wait
        numevents = aeApiPoll(eventLoop, tvp);

        // sleep后的回调，aemain死循环函数每次都设置
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
        {
            eventLoop->aftersleep(eventLoop);
        }

        for (j = 0; j < numevents; j++)
        {
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int fired = 0;
            // 通常我们首先执行可读事件，然后执行可写事件活动结束。如果在掩码中设置了AE_BARRIER，永远不要触发可写事件
            int invert = fe->mask & AE_BARRIER;

            if (!invert && fe->mask & mask & AE_READABLE)
            {
                fe->rfileProc(eventLoop, fd, fe->clientData, mask);
                fired++;
            }

            if (fe->mask & mask & AE_WRITABLE)
            {
                if (!fired || fe->wfileProc != fe->rfileProc)
                {
                    fe->wfileProc(eventLoop, fd, fe->clientData, mask);
                    fired++;
                }
            }

            // 处理翻转的可读事件
            if (invert && fe->mask & mask & AE_READABLE)
            {
                if (!fired || fe->wfileProc != fe->rfileProc)
                {
                    fe->rfileProc(eventLoop, fd, fe->clientData, mask);
                    fired++;
                }
            }

            processed++;
        }
    }
    // 检查是否有时间事件
    if (flags & AE_TIME_EVENTS)
    {
        processed += processTimeEvents(eventLoop);
    }
    // 返回处理的事件数量
    return processed;
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, long long milliseconds)
{
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE)
        pfd.events |= POLLIN;
    if (mask & AE_WRITABLE)
        pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds)) == 1)
    {
        if (pfd.revents & POLLIN)
        {
            retmask |= AE_READABLE;
        }
        if (pfd.revents & POLLOUT)
        {
            retmask |= AE_WRITABLE;
        }
        if (pfd.revents & POLLERR)
        {
            retmask |= AE_WRITABLE;
        }
        if (pfd.revents & POLLHUP)
        {
            retmask |= AE_WRITABLE;
        }
        return retmask;
    }
    else
    {
        return retval;
    }
}

void aeMain(aeEventLoop *eventLoop)
{
    eventLoop->stop = 0;
    while (!eventLoop->stop)
    {
        if (eventLoop->beforesleep != NULL)
        {
            eventLoop->beforesleep(eventLoop);
        }
        aeProcessEvents(eventLoop, AE_ALL_EVENTS | AE_CALL_AFTER_SLEEP);
    }
}

char *aeGetApiName(void)
{
    return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep)
{
    eventLoop->beforesleep = beforesleep;
}

void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep)
{
    eventLoop->aftersleep = aftersleep;
}
