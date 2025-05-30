/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#ifndef DICT_BENCHMARK_MAIN
#include "redisassert.h"
#else
#include <assert.h>
#endif

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio. */
static int dict_can_resize = 1;
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static long _dictKeyIndex(dict *ht, const void *key, uint64_t hash, dictEntry **existing);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_hash_function_seed[16];

void dictSetHashFunctionSeed(uint8_t *seed)
{
    memcpy(dict_hash_function_seed, seed, sizeof(dict_hash_function_seed));
}

uint8_t *dictGetHashFunctionSeed(void)
{
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictGenHashFunction(const void *key, int len)
{
    return siphash(key, len, dict_hash_function_seed);
}

uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len)
{
    return siphash_nocase(buf, len, dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/* Reset a hash table already initialized with ht_init().
 * NOTE: This function should only be called by ht_destroy(). */
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

dict *dictCreate(dictType *type,
                 void *privDataPtr)
{
    dict *d = zmalloc(sizeof(*d));

    _dictInit(d, type, privDataPtr);
    return d;
}

int _dictInit(dict *d, dictType *type,
              void *privDataPtr)
{
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);
    d->type = type;
    d->privdata = privDataPtr;
    d->rehashidx = -1;
    d->iterators = 0;
    return DICT_OK;
}

int dictResize(dict *d)
{
    int minimal;

    if (!dict_can_resize || dictIsRehashing(d))
    {
        return DICT_ERR;
    }
    minimal = d->ht[0].used;
    if (minimal < DICT_HT_INITIAL_SIZE)
    {
        minimal = DICT_HT_INITIAL_SIZE;
    }
    return dictExpand(d, minimal);
}

int dictExpand(dict *d, unsigned long size)
{
    // 正在扩容或者hash表的key数量大于要分配的空间
    if (dictIsRehashing(d) || d->ht[0].used > size)
    {
        return DICT_ERR;
    }

    dictht n;
    unsigned long realsize = _dictNextPower(size);

    if (realsize == d->ht[0].size)
    {
        return DICT_ERR;
    }

    // 初始化
    n.size = realsize;
    n.sizemask = realsize - 1;
    n.table = zcalloc(realsize * sizeof(dictEntry *));
    n.used = 0;

    // 应该是第一次初始化
    if (d->ht[0].table == NULL)
    {
        d->ht[0] = n;
        return DICT_OK;
    }

    // 下次开始rehash
    d->ht[1] = n;
    d->rehashidx = 0;
    return DICT_OK;
}

int dictRehash(dict *d, int n)
{
    int empty_visits = n * 10;
    if (!dictIsRehashing(d))
    {
        return 0;
    }

    while (n-- && d->ht[0].used != 0)
    {
        dictEntry *de, *nextde;

        assert(d->ht[0].size > (unsigned long)d->rehashidx);
        // 过滤buket为空的
        while (d->ht[0].table[d->rehashidx] == NULL)
        {
            d->rehashidx++;
            // 避免空bucket太多，影响主线程
            if (--empty_visits == 0)
            {
                return 1;
            }
        }
        de = d->ht[0].table[d->rehashidx];
        // 迁移该bucket的所有键值
        while (de)
        {
            uint64_t hkey;

            nextde = de->next;
            // 根据扩容后的哈希表ht[1]大小，计算当前哈希项在扩容后哈希表中的bucket位置
            hkey = dictHashKey(d, de->key) & d->ht[1].sizemask;
            // 头插法，de->next指向新节点头节点，然后头结点赋值为de
            de->next = d->ht[1].table[hkey];
            d->ht[1].table[hkey] = de;

            d->ht[0].used--;
            d->ht[1].used++;
            de = nextde;
        }
        d->ht[0].table[d->rehashidx] = NULL;
        d->rehashidx++;
    }

    // rehash完成
    if (d->ht[0].used == 0)
    {
        // table里的数据都被迁移走，直接释放table就可以
        zfree(d->ht[0].table);
        d->ht[0] = d->ht[1];
        _dictReset(&d->ht[1]);
        d->rehashidx = -1;
        return 0;
    }

    // rehash未完成，下次继续
    return 1;
}

long long timeInMilliseconds(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}

/* Rehash for an amount of time between ms milliseconds and ms+1 milliseconds */
int dictRehashMilliseconds(dict *d, int ms)
{
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while (dictRehash(d, 100))
    {
        rehashes += 100;
        if (timeInMilliseconds() - start > ms)
            break;
    }
    return rehashes;
}

// 当迭代器没有在使用的时候进行rehash
static void _dictRehashStep(dict *d)
{
    // hash迭代器没有在使用
    if (d->iterators == 0)
    {
        dictRehash(d, 1);
    }
}

int dictAdd(dict *d, void *key, void *val)
{
    dictEntry *entry = dictAddRaw(d, key, NULL);

    if (!entry)
    {
        return DICT_ERR;
    }
    dictSetVal(d, entry, val);
    return DICT_OK;
}

dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing)
{
    long index;
    dictEntry *entry;
    dictht *ht;

    if (dictIsRehashing(d))
    {
        _dictRehashStep(d);
    }

    // 获取新元素索引，-1表示已存在
    if ((index = _dictKeyIndex(d, key, dictHashKey(d, key), existing)) == -1)
    {
        return NULL;
    }

    // 新entry放在前面，插入方便，新插入的可能会更频繁被访问
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];
    entry = zmalloc(sizeof(*entry));
    entry->next = ht->table[index];
    ht->table[index] = entry;
    ht->used++;

    dictSetKey(d, entry, key);
    return entry;
}

int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, *existing, auxentry;
    // key存在返回null
    entry = dictAddRaw(d, key, &existing);
    if (entry)
    {
        dictSetVal(d, entry, val);
        return 1;
    }
    // 先设置新值再释放旧值
    auxentry = *existing;
    dictSetVal(d, existing, val);
    dictFreeVal(d, &auxentry);
    return 0;
}

/* Add or Find:
 * dictAddOrFind() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
dictEntry *dictAddOrFind(dict *d, void *key)
{
    dictEntry *entry, *existing;
    entry = dictAddRaw(d, key, &existing);
    return entry ? entry : existing;
}

// 查询到元素并移除：nofree为0则删除节点，dictDelete() 和 dictUnlink() 在使用
static dictEntry *_dictGenericDelete(dict *d, const void *key, int nofree)
{
    uint64_t hkey, idx;
    dictEntry *he, *prevHe;
    int table;

    if (d->ht[0].used == 0 && d->ht[1].used == 0)
    {
        return NULL;
    }

    if (dictIsRehashing(d))
    {
        _dictRehashStep(d);
    }
    hkey = dictHashKey(d, key);

    for (table = 0; table <= 1; table++)
    {
        idx = hkey & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        prevHe = NULL;
        while (he)
        {
            if (key == he->key || dictCompareKeys(d, key, he->key))
            {
                // 从list移除节点
                if (prevHe)
                {
                    prevHe->next = he->next;
                }
                else
                {
                    d->ht[table].table[idx] = he->next;
                }
                if (!nofree)
                {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                    zfree(he);
                }
                d->ht[table].used--;
                return he;
            }
            prevHe = he;
            he = he->next;
        }
        if (!dictIsRehashing(d))
        {
            break;
        }
    }
    return NULL;
}

int dictDelete(dict *ht, const void *key)
{
    return _dictGenericDelete(ht, key, 0) ? DICT_OK : DICT_ERR;
}

dictEntry *dictUnlink(dict *ht, const void *key)
{
    return _dictGenericDelete(ht, key, 1);
}

void dictFreeUnlinkedEntry(dict *d, dictEntry *he)
{
    if (he == NULL)
    {
        return;
    }
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}

// 清空 dict
int _dictClear(dict *d, dictht *ht, void(callback)(void *))
{
    unsigned long i;

    for (i = 0; i < ht->size && ht->used > 0; i++)
    {
        dictEntry *he, *nextHe;

        // i & 0xFFFF == 0 就是在第一次或者每释放65535个元素的时候回调一次
        // 在清空大字典时定期通知调用方进度或者让调用方有机会在长时间操作中执行其他任务
        if (callback && (i & 65535) == 0)
        {
            callback(d->privdata);
        }

        if ((he = ht->table[i]) == NULL)
        {
            continue;
        }
        while (he)
        {
            nextHe = he->next;
            dictFreeKey(d, he);
            dictFreeVal(d, he);
            zfree(he);
            ht->used--;
            he = nextHe;
        }
    }

    zfree(ht->table);
    _dictReset(ht);
    return DICT_OK;
}

void dictRelease(dict *d)
{
    _dictClear(d, &d->ht[0], NULL);
    _dictClear(d, &d->ht[1], NULL);
    zfree(d);
}

dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    uint64_t hkey, idx, table;

    if (d->ht[0].used + d->ht[1].used == 0)
    {
        return NULL;
    }
    if (dictIsRehashing(d))
    {
        _dictRehashStep(d);
    }
    hkey = dictHashKey(d, key);
    for (table = 0; table <= 1; table++)
    {
        idx = hkey & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        while (he)
        {
            if (key == he->key || dictCompareKeys(d, key, he->key))
            {
                return he;
            }
            he = he->next;
        }
        if (!dictIsRehashing(d))
        {
            return NULL;
        }
    }
    return NULL;
}

void *dictFetchValue(dict *d, const void *key)
{
    dictEntry *he;

    he = dictFind(d, key);
    return he ? dictGetVal(he) : NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictionary
 * at a given time, it's just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating. */
long long _dictFingerprint(dict *d)
{
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long)d->ht[0].table;
    integers[1] = d->ht[0].size;
    integers[2] = d->ht[0].used;
    integers[3] = (long)d->ht[1].table;
    integers[4] = d->ht[1].size;
    integers[5] = d->ht[1].used;

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++)
    {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

dictIterator *dictGetSafeIterator(dict *d)
{
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

dictEntry *dictNext(dictIterator *iter)
{
    while (1)
    {
        if (iter->entry == NULL)
        {
            // 第一次调用
            dictht *ht = &iter->d->ht[iter->table];
            if (iter->index == -1 && iter->table == 0)
            {
                if (iter->safe)
                {
                    iter->d->iterators++;
                }
                else
                {
                    // 防止迭代过程中的意外修改
                    iter->fingerprint = _dictFingerprint(iter->d);
                }
            }
            iter->index++;
            if (iter->index >= (long)ht->size)
            {
                if (dictIsRehashing(iter->d) && iter->table == 0)
                {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                }
                else
                {
                    break;
                }
            }
            iter->entry = ht->table[iter->index];
        }
        else
        {
            iter->entry = iter->nextEntry;
        }
        if (iter->entry)
        {
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL;
}

void dictReleaseIterator(dictIterator *iter)
{
    if (!(iter->index == -1 && iter->table == 0))
    {
        if (iter->safe)
        {
            iter->d->iterators--;
        }
        else
        {
            assert(iter->fingerprint == _dictFingerprint(iter->d));
        }
    }
    zfree(iter);
}

dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned long hkey;
    int listlen, listele;

    if (dictSize(d) == 0)
    {
        return NULL;
    }
    if (dictIsRehashing(d))
    {
        _dictRehashStep(d);
    }
    if (dictIsRehashing(d))
    {
        do
        {
            // 0到rehashidx-1里的key都被迁移了，直接跳过
            hkey = d->rehashidx + (random() % (d->ht[0].size +
                                               d->ht[1].size -
                                               d->rehashidx));
            he = (hkey >= d->ht[0].size) ? d->ht[1].table[hkey - d->ht[0].size] : d->ht[0].table[hkey];
        } while (he == NULL);
    }
    else
    {
        do
        {
            hkey = random() & d->ht[0].sizemask;
            he = d->ht[0].table[hkey];
        } while (he == NULL);
    }

    // 从entry的链表里再随机选一个
    listlen = 0;
    orighe = he;
    while (he)
    {
        he = he->next;
        listlen++;
    }
    listele = random() % listlen;
    he = orighe;
    while (listele--)
    {
        he = he->next;
    }
    return he;
}
// count在统计时默认配置是5
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count)
{
    unsigned long j;      /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    if (dictSize(d) < count)
    {
        count = dictSize(d);
    }
    maxsteps = count * 10;

    // 成比例的rehash
    for (j = 0; j < count; j++)
    {
        if (dictIsRehashing(d))
        {
            _dictRehashStep(d);
        }
        else
        {
            break;
        }
    }

    tables = dictIsRehashing(d) ? 2 : 1;
    maxsizemask = d->ht[0].sizemask;
    if (tables > 1 && maxsizemask < d->ht[1].sizemask)
    {
        maxsizemask = d->ht[1].sizemask;
    }

    unsigned long randKey = random() & maxsizemask;
    unsigned long emptylen = 0;
    while (stored < count && maxsteps--)
    {
        for (j = 0; j < tables; j++)
        {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            if (tables == 2 && j == 0 && randKey < (unsigned long)d->rehashidx)
            {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (randKey >= d->ht[1].size)
                {
                    randKey = d->rehashidx;
                }
                else
                {
                    continue;
                }
            }
            if (randKey >= d->ht[j].size)
            {
                continue;
            }

            dictEntry *he = d->ht[j].table[randKey];

            if (he == NULL)
            {
                emptylen++;
                if (emptylen >= 5 && emptylen > count)
                {
                    randKey = random() & maxsizemask;
                    emptylen = 0;
                }
            }
            else
            {
                emptylen = 0;
                while (he)
                {
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    if (stored == count)
                    {
                        return stored;
                    }
                }
            }
        }
        randKey = (randKey + 1) & maxsizemask;
    }
    return stored;
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static unsigned long rev(unsigned long v)
{
    unsigned long s = 8 * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0;
    while ((s >>= 1) > 0)
    {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

/* dictScan() is used to iterate over the elements of a dictionary.
 *
 * Iterating works the following way:
 *
 * 1) Initially you call the function using a cursor (v) value of 0.
 * 2) The function performs one step of the iteration, and returns the
 *    new cursor value you must use in the next call.
 * 3) When the returned cursor is 0, the iteration is complete.
 *
 * The function guarantees all elements present in the
 * dictionary get returned between the start and end of the iteration.
 * However it is possible some elements get returned multiple times.
 *
 * For every element returned, the callback argument 'fn' is
 * called with 'privdata' as first argument and the dictionary entry
 * 'de' as second argument.
 *
 * HOW IT WORKS.
 *
 * The iteration algorithm was designed by Pieter Noordhuis.
 * The main idea is to increment a cursor starting from the higher order
 * bits. That is, instead of incrementing the cursor normally, the bits
 * of the cursor are reversed, then the cursor is incremented, and finally
 * the bits are reversed again.
 *
 * This strategy is needed because the hash table may be resized between
 * iteration calls.
 *
 * dict.c hash tables are always power of two in size, and they
 * use chaining, so the position of an element in a given table is given
 * by computing the bitwise AND between Hash(key) and SIZE-1
 * (where SIZE-1 is always the mask that is equivalent to taking the rest
 *  of the division between the Hash of the key and SIZE).
 *
 * For example if the current hash table size is 16, the mask is
 * (in binary) 1111. The position of a key in the hash table will always be
 * the last four bits of the hash output, and so forth.
 *
 * WHAT HAPPENS IF THE TABLE CHANGES IN SIZE?
 *
 * If the hash table grows, elements can go anywhere in one multiple of
 * the old bucket: for example let's say we already iterated with
 * a 4 bit cursor 1100 (the mask is 1111 because hash table size = 16).
 *
 * If the hash table will be resized to 64 elements, then the new mask will
 * be 111111. The new buckets you obtain by substituting in ??1100
 * with either 0 or 1 can be targeted only by keys we already visited
 * when scanning the bucket 1100 in the smaller hash table.
 *
 * By iterating the higher bits first, because of the inverted counter, the
 * cursor does not need to restart if the table size gets bigger. It will
 * continue iterating using cursors without '1100' at the end, and also
 * without any other combination of the final 4 bits already explored.
 *
 * Similarly when the table size shrinks over time, for example going from
 * 16 to 8, if a combination of the lower three bits (the mask for size 8
 * is 111) were already completely explored, it would not be visited again
 * because we are sure we tried, for example, both 0111 and 1111 (all the
 * variations of the higher bit) so we don't need to test it again.
 *
 * WAIT... YOU HAVE *TWO* TABLES DURING REHASHING!
 *
 * Yes, this is true, but we always iterate the smaller table first, then
 * we test all the expansions of the current cursor into the larger
 * table. For example if the current cursor is 101 and we also have a
 * larger table of size 16, we also test (0)101 and (1)101 inside the larger
 * table. This reduces the problem back to having only one table, where
 * the larger one, if it exists, is just an expansion of the smaller one.
 *
 * LIMITATIONS
 *
 * This iterator is completely stateless, and this is a huge advantage,
 * including no additional memory used.
 *
 * The disadvantages resulting from this design are:
 *
 * 1) It is possible we return elements more than once. However this is usually
 *    easy to deal with in the application level.
 * 2) The iterator must return multiple elements per call, as it needs to always
 *    return all the keys chained in a given bucket, and all the expansions, so
 *    we are sure we don't miss keys moving during rehashing.
 * 3) The reverse cursor is somewhat hard to understand at first, but this
 *    comment is supposed to help.
 */
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       dictScanBucketFunction *bucketfn,
                       void *privdata)
{
    dictht *t0, *t1;
    const dictEntry *de, *next;
    unsigned long m0, m1;

    if (dictSize(d) == 0)
        return 0;

    if (!dictIsRehashing(d))
    {
        t0 = &(d->ht[0]);
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        if (bucketfn)
            bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de)
        {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Set unmasked bits so incrementing the reversed cursor
         * operates on the masked bits */
        v |= ~m0;

        /* Increment the reverse cursor */
        v = rev(v);
        v++;
        v = rev(v);
    }
    else
    {
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        if (t0->size > t1->size)
        {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        if (bucketfn)
            bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de)
        {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        do
        {
            /* Emit entries at cursor */
            if (bucketfn)
                bucketfn(privdata, &t1->table[v & m1]);
            de = t1->table[v & m1];
            while (de)
            {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment the reverse cursor not covered by the smaller mask.*/
            v |= ~m1;
            v = rev(v);
            v++;
            v = rev(v);

            /* Continue while bits covered by mask difference is non-zero */
        } while (v & (m0 ^ m1));
    }

    return v;
}

/* ------------------------- private functions ------------------------------ */

// 是否需要扩容
static int _dictExpandIfNeeded(dict *d)
{
    // 正在rehash
    if (dictIsRehashing(d))
    {
        return DICT_OK;
    }

    // 初始化
    if (d->ht[0].size == 0)
    {
        return dictExpand(d, DICT_HT_INITIAL_SIZE);
    }
    // dict_force_resize_ratio 负载因子是5
    // dict_can_resize==1 当前没有 RDB 子进程且也没有 AOF 子进程
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used / d->ht[0].size > dict_force_resize_ratio))
    {
        return dictExpand(d, d->ht[0].used * 2);
    }
    return DICT_OK;
}

// 2倍现有空间扩容，大小不能超过LONG_MAX
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX)
        return LONG_MAX + 1LU;
    while (1)
    {
        if (i >= size)
            return i;
        i *= 2;
    }
}

// 如果key存在返回-1，existing就是那个dictEntry，返回可以填充给定“key” 的 index
// 如果正在rehash返回的是新的table
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing)
{
    unsigned long idx, table;
    dictEntry *he;
    if (existing)
    {
        *existing = NULL;
    }

    // 扩容判断
    if (_dictExpandIfNeeded(d) == DICT_ERR)
    {
        return -1;
    }
    for (table = 0; table <= 1; table++)
    {
        idx = hash & d->ht[table].sizemask;
        //
        he = d->ht[table].table[idx];
        while (he)
        {
            // 用 || 是因为只需要找到slot就可以
            if (key == he->key || dictCompareKeys(d, key, he->key))
            {
                if (existing)
                {
                    *existing = he;
                }
                return -1;
            }
            he = he->next;
        }
        // 如果没有在rehash就不需要查table[1]
        if (!dictIsRehashing(d))
        {
            break;
        }
    }
    return idx;
}

void dictEmpty(dict *d, void(callback)(void *))
{
    _dictClear(d, &d->ht[0], callback);
    _dictClear(d, &d->ht[1], callback);
    d->rehashidx = -1;
    d->iterators = 0;
}

void dictEnableResize(void)
{
    dict_can_resize = 1;
}

void dictDisableResize(void)
{
    dict_can_resize = 0;
}

uint64_t dictGetHash(dict *d, const void *key)
{
    return dictHashKey(d, key);
}

dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash)
{
    dictEntry *he, **heref;
    unsigned long hkey, table;

    if (dictSize(d) == 0)
    {
        return NULL;
    }
    for (table = 0; table <= 1; table++)
    {
        hkey = hash & d->ht[table].sizemask;
        heref = &d->ht[table].table[hkey];
        he = *heref;
        while (he)
        {
            if (oldptr == he->key)
            {
                return heref;
            }
            he = he->next;
        }
        if (!dictIsRehashing(d))
        {
            return NULL;
        }
    }
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50
size_t _dictGetStatsHt(char *buf, size_t bufsize, dictht *ht, int tableid)
{
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];
    size_t l = 0;

    if (ht->used == 0)
    {
        return snprintf(buf, bufsize,
                        "No stats available for empty dictionaries\n");
    }

    for (i = 0; i < DICT_STATS_VECTLEN; i++)
    {
        clvector[i] = 0;
    }
    for (i = 0; i < ht->size; i++)
    {
        dictEntry *he;

        if (ht->table[i] == NULL)
        {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while (he)
        {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN - 1)]++;
        if (chainlen > maxchainlen)
        {
            maxchainlen = chainlen;
        }
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf + l, bufsize - l,
                  "Hash table %d stats (%s):\n"
                  " table size: %ld\n"
                  " number of elements: %ld\n"
                  " different slots: %ld\n"
                  " max chain length: %ld\n"
                  " avg chain length (counted): %.02f\n"
                  " avg chain length (computed): %.02f\n"
                  " Chain length distribution:\n",
                  tableid, (tableid == 0) ? "main hash table" : "rehashing target",
                  ht->size, ht->used, slots, maxchainlen,
                  (float)totchainlen / slots, (float)ht->used / slots);

    for (i = 0; i < DICT_STATS_VECTLEN - 1; i++)
    {
        if (clvector[i] == 0)
            continue;
        if (l >= bufsize)
            break;
        l += snprintf(buf + l, bufsize - l,
                      "   %s%ld: %ld (%.02f%%)\n",
                      (i == DICT_STATS_VECTLEN - 1) ? ">= " : "",
                      i, clvector[i], ((float)clvector[i] / ht->size) * 100);
    }

    if (bufsize)
    {
        buf[bufsize - 1] = '\0';
    }
    return strlen(buf);
}

void dictGetStats(char *buf, size_t bufsize, dict *d)
{
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf, bufsize, &d->ht[0], 0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0)
    {
        _dictGetStatsHt(buf, bufsize, &d->ht[1], 1);
    }
    if (orig_bufsize)
    {
        orig_buf[orig_bufsize - 1] = '\0';
    }
}

/* ------------------------------- Benchmark ---------------------------------*/
// gcc -g -o dictmain dict.c sds.c zmalloc.c siphash.c -DDICT_BENCHMARK_MAIN
#ifdef DICT_BENCHMARK_MAIN

#include "sds.h"

uint64_t hashCallback(const void *key)
{
    return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

int compareCallback(void *privdata, const void *key1, const void *key2)
{
    int l1, l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2)
        return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg)                                      \
    do                                                          \
    {                                                           \
        elapsed = timeInMilliseconds() - start;                 \
        printf(msg ": %ld items in %lld ms\n", count, elapsed); \
    } while (0);

/* dict-benchmark [count] */
int main(int argc, char **argv)
{
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType, NULL);
    long count = 0;

    if (argc == 2)
    {
        count = strtol(argv[1], NULL, 10);
    }
    else
    {
        count = 50;
    }

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        int retval = dictAdd(dict, sdsfromlonglong(j), (void *)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);
    // 统计
    char buf[1024];
    dictGetStats(buf, sizeof(buf), dict);
    printf("dictGetStats dict: %s \n", buf);
    // 迭代器遍历
    dictIterator *diter = dictGetSafeIterator(dict);
    dictEntry *de;
    while (de = dictNext(diter))
    {
        // printf("%s:%d ", (char *)de->key, (void *)de->v);
    }
    dictReleaseIterator(diter);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict))
    {
        dictRehashMilliseconds(dict, 100);
    }

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict, key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict, key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(rand() % count);
        dictEntry *de = dictFind(dict, key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict, key);
        assert(de == NULL);
        sdsfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++)
    {
        sds key = sdsfromlonglong(j);
        int retval = dictDelete(dict, key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict, key, (void *)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
}
#endif
