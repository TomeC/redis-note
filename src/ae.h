/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __AE_H__
#define __AE_H__

#include <time.h>

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0     // 无事件
#define AE_READABLE 1 // 触发读事件
#define AE_WRITABLE 2 // 触发写事件
#define AE_BARRIER 4  // 阻止读事件的发送，比如先持久化再返回

#define AE_FILE_EVENTS 1 // 1
#define AE_TIME_EVENTS 2 // 10
#define AE_ALL_EVENTS (AE_FILE_EVENTS | AE_TIME_EVENTS)
#define AE_DONT_WAIT 4        // 100
#define AE_CALL_AFTER_SLEEP 8 // 1000

#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void)V)

struct aeEventLoop;

/* Types and data structures */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

typedef struct aeFileEvent
{
    int mask;              // AE_(READABLE|WRITABLE|BARRIER) 其中之一
    aeFileProc *rfileProc; // read 回调函数
    aeFileProc *wfileProc; // write 回调函数
    void *clientData;      // 客户端私有数据
} aeFileEvent;

typedef struct aeTimeEvent
{
    long long id;                        // 时间事件ID
    long when_sec;                       // 事件到达的秒级时间戳
    long when_ms;                        // 事件到达的毫秒级时间戳
    aeTimeProc *timeProc;                // 时间事件触发后的处理函数
    aeEventFinalizerProc *finalizerProc; // 事件结束后的处理函数
    void *clientData;                    // 事件相关的私有数据
    struct aeTimeEvent *prev;            // 时间事件链表的前向指针
    struct aeTimeEvent *next;            // 时间事件链表的后向指针
} aeTimeEvent;

/* A fired event */
typedef struct aeFiredEvent
{
    int fd;
    int mask;
} aeFiredEvent;

/* State of an event based program */
typedef struct aeEventLoop
{
    int maxfd;   // 初始化为-1，监听端口返回的fd
    int setsize; // 能监听的最大文件fd数量 server.maxclients + CONFIG_FDSET_INCR
    long long timeEventNextId;
    time_t lastTime;            /* Used to detect system clock skew */
    aeFileEvent *events;        // 注册事件
    aeFiredEvent *fired;        // 被触发的事件
    aeTimeEvent *timeEventHead; // 记录时间事件的链表
    int stop;
    void *apidata;                  // epoll的aeApiState
    aeBeforeSleepProc *beforesleep; // service.c 里的 beforeSleep
    aeBeforeSleepProc *aftersleep;  // service.c 里的 afterSleep
} aeEventLoop;

// 接口
// main函数中初始化事件循环，setsize默认1128，初始化并调用epoll_create
aeEventLoop *aeCreateEventLoop(int setsize);
// 然后调用server.c:listenToPort
// 事件注册函数
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask, aeFileProc *proc, void *clientData);
// 释放事件循环资源
void aeDeleteEventLoop(aeEventLoop *eventLoop);
// 停止事件循环
void aeStop(aeEventLoop *eventLoop);

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);

#endif
