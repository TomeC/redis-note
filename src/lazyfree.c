#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static size_t lazyfree_objects = 0; // 全部需要释放度饿对象
pthread_mutex_t lazyfree_objects_mutex = PTHREAD_MUTEX_INITIALIZER;

// 返回待释放的对象数量
size_t lazyfreeGetPendingObjectsCount(void)
{
    size_t aux;
    atomicGet(lazyfree_objects, aux);
    return aux;
}

// 获取对象对应的元素数量
size_t lazyfreeGetFreeEffort(robj *obj)
{
    if (obj->type == OBJ_LIST)
    {
        quicklist *ql = obj->ptr;
        return ql->len;
    }
    else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT)
    {
        dict *ht = obj->ptr;
        return dictSize(ht);
    }
    else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zset *zs = obj->ptr;
        return zs->zsl->length;
    }
    else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT)
    {
        dict *ht = obj->ptr;
        return dictSize(ht);
    }
    else
    {
        return 1;
    }
}

// 如果待释放的资源过大就异步处理
#define LAZYFREE_THRESHOLD 64
int dbAsyncDelete(redisDb *db, robj *key)
{
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if (dictSize(db->expires) > 0)
    {
        dictDelete(db->expires, key->ptr);
    }

    /* If the value is composed of a few allocations, to free in a lazy way
     * is actually just slower... So under a certain limit we just free
     * the object synchronously. */
    dictEntry *de = dictUnlink(db->dict, key->ptr);
    if (de)
    {
        robj *val = dictGetVal(de);
        size_t free_effort = lazyfreeGetFreeEffort(val);

        /* If releasing the object is too much work, do it in the background
         * by adding the object to the lazy free list.
         * Note that if the object is shared, to reclaim it now it is not
         * possible. This rarely happens, however sometimes the implementation
         * of parts of the Redis core may call incrRefCount() to protect
         * objects, and then call dbDelete(). In this case we'll fall
         * through and reach the dictFreeUnlinkedEntry() call, that will be
         * equivalent to just calling decrRefCount(). */
        if (free_effort > LAZYFREE_THRESHOLD && val->refcount == 1)
        {
            atomicIncr(lazyfree_objects, 1);
            bioCreateBackgroundJob(BIO_LAZY_FREE, val, NULL, NULL);
            dictSetVal(db->dict, de, NULL);
        }
    }

    /* Release the key-val pair, or just the key if we set the val
     * field to NULL in order to lazy free it later. */
    if (de)
    {
        dictFreeUnlinkedEntry(db->dict, de);
        if (server.cluster_enabled)
        {
            slotToKeyDel(key);
        }
        return 1;
    }
    else
    {
        return 0;
    }
}

// 如果对象过大就异步释放
void freeObjAsync(robj *o)
{
    size_t free_effort = lazyfreeGetFreeEffort(o);
    if (free_effort > LAZYFREE_THRESHOLD && o->refcount == 1)
    {
        atomicIncr(lazyfree_objects, 1);
        bioCreateBackgroundJob(BIO_LAZY_FREE, o, NULL, NULL);
    }
    else
    {
        decrRefCount(o);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
void emptyDbAsync(redisDb *db)
{
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    db->dict = dictCreate(&dbDictType, NULL);
    db->expires = dictCreate(&keyptrDictType, NULL);
    atomicIncr(lazyfree_objects, dictSize(oldht1));
    bioCreateBackgroundJob(BIO_LAZY_FREE, NULL, oldht1, oldht2);
}

/* Empty the slots-keys map of Redis CLuster by creating a new empty one
 * and scheduiling the old for lazy freeing. */
void slotToKeyFlushAsync(void)
{
    rax *old = server.cluster->slots_to_keys;

    server.cluster->slots_to_keys = raxNew();
    memset(server.cluster->slots_keys_count, 0, sizeof(server.cluster->slots_keys_count));
    atomicIncr(lazyfree_objects, old->numele);
    bioCreateBackgroundJob(BIO_LAZY_FREE, NULL, NULL, old);
}

// 后台线程调用，释放对象
void lazyfreeFreeObjectFromBioThread(robj *o)
{
    decrRefCount(o);
    atomicDecr(lazyfree_objects, 1);
}

// 后台线程中 释放 dict 全部数据
void lazyfreeFreeDatabaseFromBioThread(dict *ht1, dict *ht2)
{
    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    dictRelease(ht2);
    atomicDecr(lazyfree_objects, numkeys);
}

// 后台线程中 释放 radix tree 全部数据
void lazyfreeFreeSlotsMapFromBioThread(rax *rt)
{
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects, len);
}
