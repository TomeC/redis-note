/* Background I/O service for Redis.
 *
 * This file implements operations that we need to perform in the background.
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread waits for new jobs in its queue, and process every job
 * sequentially.
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * ----------------------------------------------------------------------------
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

#include "server.h"
#include "bio.h"

static pthread_t bio_threads[BIO_NUM_OPS];
static pthread_mutex_t bio_mutex[BIO_NUM_OPS];
static pthread_cond_t bio_newjob_cond[BIO_NUM_OPS];
static pthread_cond_t bio_step_cond[BIO_NUM_OPS];
static list *bio_jobs[BIO_NUM_OPS];
// 以下数组用于保存每个任务的待处理作业数OP类型。对于报告也很有用
static unsigned long long bio_pending[BIO_NUM_OPS];

// 内部使用的任务结构体
struct bio_job
{
    // 任务被创建的时间
    time_t time;
    // 任务参数，如果参数超过三个，可以传指向结构体的指针
    void *arg1, *arg2, *arg3;
};

void *bioProcessBackgroundJobs(void *arg);
void lazyfreeFreeObjectFromBioThread(robj *o);
void lazyfreeFreeDatabaseFromBioThread(dict *ht1, dict *ht2);
void lazyfreeFreeSlotsMapFromBioThread(zskiplist *sl);

// 线程栈设置为4M，避免栈空间不足
#define REDIS_THREAD_STACK_SIZE (1024 * 1024 * 4)

void bioInit(void)
{
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    for (j = 0; j < BIO_NUM_OPS; j++)
    {
        pthread_mutex_init(&bio_mutex[j], NULL);
        pthread_cond_init(&bio_newjob_cond[j], NULL);
        pthread_cond_init(&bio_step_cond[j], NULL);
        bio_jobs[j] = listCreate();
        bio_pending[j] = 0;
    }

    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize)
    {
        stacksize = 1; /* The world is full of Solaris Fixes */
    }

    while (stacksize < REDIS_THREAD_STACK_SIZE)
    {
        stacksize *= 2;
    }
    pthread_attr_setstacksize(&attr, stacksize);

    for (j = 0; j < BIO_NUM_OPS; j++)
    {
        void *arg = (void *)(unsigned long)j;
        if (pthread_create(&thread, &attr, bioProcessBackgroundJobs, arg) != 0)
        {
            serverLog(LL_WARNING, "Fatal: Can't initialize Background Jobs.");
            exit(1);
        }
        bio_threads[j] = thread;
    }
}

void bioCreateBackgroundJob(int type, void *arg1, void *arg2, void *arg3)
{
    struct bio_job *job = zmalloc(sizeof(*job));

    job->time = time(NULL);
    job->arg1 = arg1;
    job->arg2 = arg2;
    job->arg3 = arg3;
    pthread_mutex_lock(&bio_mutex[type]);
    listAddNodeTail(bio_jobs[type], job);
    bio_pending[type]++;
    pthread_cond_signal(&bio_newjob_cond[type]);
    pthread_mutex_unlock(&bio_mutex[type]);
}
// 3个线程，每个都会执行
void *bioProcessBackgroundJobs(void *arg)
{
    struct bio_job *job;
    unsigned long type = (unsigned long)arg;
    sigset_t sigset;

    // 校验类型合法性
    if (type >= BIO_NUM_OPS)
    {
        serverLog(LL_WARNING, "Warning: bio thread started with wrong type %lu", type);
        return NULL;
    }

    // 设置线程为可在任意时刻取消
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    pthread_mutex_lock(&bio_mutex[type]);

    // 阻止当前后台线程响应 SIGALRM 信号，确保只有主线程能收到这个定时器信号
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
    {
        serverLog(LL_WARNING, "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));
    }

    while (1)
    {
        listNode *ln;

        // 任务队列为空就阻塞
        if (listLength(bio_jobs[type]) == 0)
        {
            pthread_cond_wait(&bio_newjob_cond[type], &bio_mutex[type]);
            continue;
        }
        // 获取首节点
        ln = listFirst(bio_jobs[type]);
        job = ln->value;
        // 解锁队列
        pthread_mutex_unlock(&bio_mutex[type]);

        // 任务处理
        if (type == BIO_CLOSE_FILE)
        {
            // aof里关闭文件，可能会很慢
            close((long)job->arg1);
        }
        else if (type == BIO_AOF_FSYNC)
        {
            // 调用fdatasync系统接口只同步部分元数据，性能好
            redis_fsync((long)job->arg1);
        }
        else if (type == BIO_LAZY_FREE)
        {
            // 释放资源
            if (job->arg1)
            {
                lazyfreeFreeObjectFromBioThread(job->arg1);
            }
            else if (job->arg2 && job->arg3)
            {
                lazyfreeFreeDatabaseFromBioThread(job->arg2, job->arg3);
            }
            else if (job->arg3)
            {
                lazyfreeFreeSlotsMapFromBioThread(job->arg3);
            }
        }
        else
        {
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }
        zfree(job);

        // 队列加锁，然后删除任务，唤醒所有等待线程
        pthread_mutex_lock(&bio_mutex[type]);
        listDelNode(bio_jobs[type], ln);
        bio_pending[type]--;

        pthread_cond_broadcast(&bio_step_cond[type]);
    }
}

unsigned long long bioPendingJobsOfType(int type)
{
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

unsigned long long bioWaitStepOfType(int type)
{
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    if (val != 0)
    {
        pthread_cond_wait(&bio_step_cond[type], &bio_mutex[type]);
        val = bio_pending[type];
    }
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory.
 debug.c 用到了*/
void bioKillThreads(void)
{
    int err, j;

    for (j = 0; j < BIO_NUM_OPS; j++)
    {
        if (pthread_cancel(bio_threads[j]) == 0)
        {
            if ((err = pthread_join(bio_threads[j], NULL)) != 0)
            {
                serverLog(LL_WARNING,
                          "Bio thread for job type #%d can be joined: %s",
                          j, strerror(err));
            }
            else
            {
                serverLog(LL_WARNING,
                          "Bio thread for job type #%d terminated", j);
            }
        }
    }
}
