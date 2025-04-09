/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
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

#include <stdint.h>

#ifndef __DICT_H
#define __DICT_H

#define DICT_OK 0
#define DICT_ERR 1

// 未使用变量避免告警
#define DICT_NOTUSED(V) ((void)V)

typedef struct dictEntry
{
    void *key;
    union
    {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct dictEntry *next;
} dictEntry;

typedef struct dictType
{
    uint64_t (*hashFunction)(const void *key);
    void *(*keyDup)(void *privdata, const void *key);
    void *(*valDup)(void *privdata, const void *obj);
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
    void (*keyDestructor)(void *privdata, void *key);
    void (*valDestructor)(void *privdata, void *obj);
} dictType;

/* This is our hash table structure. Every dictionary has two of this as we
 * implement incremental rehashing, for the old to the new table. */
typedef struct dictht
{
    dictEntry **table;
    unsigned long size;
    unsigned long sizemask; // 0x...FF
    unsigned long used;
} dictht;

typedef struct dict
{
    dictType *type;
    void *privdata;
    dictht ht[2];
    long rehashidx;          // 当前 rehash 在对哪个 bucket 做数据迁移，-1没有在迁移
    unsigned long iterators; // 当前迭代器数量
} dict;

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext()
 * should be called while iterating. */
typedef struct dictIterator
{
    dict *d;
    long index;
    int table, safe;
    dictEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint;
} dictIterator;

typedef void(dictScanFunction)(void *privdata, const dictEntry *de);
typedef void(dictScanBucketFunction)(void *privdata, dictEntry **bucketref);

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE 4

/* ------------------------------- Macros ------------------------------------*/
#define dictFreeVal(d, entry)     \
    if ((d)->type->valDestructor) \
    (d)->type->valDestructor((d)->privdata, (entry)->v.val)

#define dictSetVal(d, entry, _val_)                                   \
    do                                                                \
    {                                                                 \
        if ((d)->type->valDup)                                        \
            (entry)->v.val = (d)->type->valDup((d)->privdata, _val_); \
        else                                                          \
            (entry)->v.val = (_val_);                                 \
    } while (0)

#define dictSetSignedIntegerVal(entry, _val_) \
    do                                        \
    {                                         \
        (entry)->v.s64 = _val_;               \
    } while (0)

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do                                          \
    {                                           \
        (entry)->v.u64 = _val_;                 \
    } while (0)

#define dictSetDoubleVal(entry, _val_) \
    do                                 \
    {                                  \
        (entry)->v.d = _val_;          \
    } while (0)

#define dictFreeKey(d, entry)     \
    if ((d)->type->keyDestructor) \
    (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define dictSetKey(d, entry, _key_)                                 \
    do                                                              \
    {                                                               \
        if ((d)->type->keyDup)                                      \
            (entry)->key = (d)->type->keyDup((d)->privdata, _key_); \
        else                                                        \
            (entry)->key = (_key_);                                 \
    } while (0)

#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? (d)->type->keyCompare((d)->privdata, key1, key2) : (key1) == (key2))

#define dictHashKey(d, key) (d)->type->hashFunction(key)
#define dictGetKey(he) ((he)->key)
#define dictGetVal(he) ((he)->v.val)
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)
#define dictSlots(d) ((d)->ht[0].size + (d)->ht[1].size)
#define dictSize(d) ((d)->ht[0].used + (d)->ht[1].used)
#define dictIsRehashing(d) ((d)->rehashidx != -1)

/* API */
// 创建一个新的hash表
dict *dictCreate(dictType *type, void *privDataPtr);
// 添加一个元素到hash表，返回DICT_OK或DICT_ERR
int dictAdd(dict *d, void *key, void *val);
// 创建或扩容hash
int dictExpand(dict *d, unsigned long size);
// 底层接口：查询key，如果不存在就插入
// 如果key已存在返回null并赋值existing，不存在则返回赋值key但value为空的dictEntry对象
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);
// 查询key，如果不存在就插入
// 返回赋值key但value为空的dictEntry对象
dictEntry *dictAddOrFind(dict *d, void *key);
// 插入或覆盖：key不存在就插入并返回0，存在就设置新value释放旧value并返回1
int dictReplace(dict *d, void *key, void *val);
// 删除元素，成功返回DICT_OK，key不存在返回 DICT_ERR
int dictDelete(dict *d, const void *key);
// 从table中移除元素，但不释放。如果找到key对应的entry在使用完后需要调用dictFreeUnlinkedEntry()释放资源，没找到返回null
dictEntry *dictUnlink(dict *ht, const void *key);
// 释放 dictUnlink() 返回的entry
void dictFreeUnlinkedEntry(dict *d, dictEntry *he);
// 释放hash
void dictRelease(dict *d);
// 查找key对应的entry，不存在返回null
dictEntry *dictFind(dict *d, const void *key);
// 查找key对应的value，不存在返回null
void *dictFetchValue(dict *d, const void *key);
// table缩容到能包括所有元素的最小大小，最小是4
int dictResize(dict *d);
// 获取迭代器
dictIterator *dictGetIterator(dict *d);
// 获取安全迭代器，允许在迭代过程中对字典进行修改
dictIterator *dictGetSafeIterator(dict *d);
// 获取下一个迭代器，结束返回NULL
dictEntry *dictNext(dictIterator *iter);
// 释放迭代器
void dictReleaseIterator(dictIterator *iter);
// 获取随机的entry，用在随机算法里
dictEntry *dictGetRandomKey(dict *d);
// todo 从字典中随机获取一定数量的键，避免全表扫描【？？不是很明白】
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);
// dict统计 debug使用
void dictGetStats(char *buf, size_t bufsize, dict *d);
// 设置hash seed
void dictSetHashFunctionSeed(uint8_t *seed);
// 获取hash seed
uint8_t *dictGetHashFunctionSeed(void);
// 计算key的hash值
uint64_t dictGenHashFunction(const void *key, int len);
// 计算字符串的hash值【不区分大小写】
uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len);
// 清空 dict
void dictEmpty(dict *d, void(callback)(void *));
// 设置允许resize标识
void dictEnableResize(void);
// 设置禁止resize标识
void dictDisableResize(void);
// 执行N步rehash。如果仍然rehash未完成，则返回1，否则返回0。
// 在迁移N个bucket的时候，最多检查N*10个空bucket，避免影响主线程业务
int dictRehash(dict *d, int n);
// 指定时间内进行rehash
int dictRehashMilliseconds(dict *d, int ms);

unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);
// 获取key对应的hash值
uint64_t dictGetHash(dict *d, const void *key);
// 通过key指针和哈希值来高效查找
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif /* __DICT_H */
