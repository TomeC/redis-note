/*
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "cluster.h"
#include "rdb.h"
#include <dlfcn.h>

#define REDISMODULE_CORE 1
#include "redismodule.h"

/* --------------------------------------------------------------------------
 * Private data structures used by the modules system. Those are data
 * structures that are never exposed to Redis Modules, if not as void
 * pointers that have an API the module can call with them)
 * -------------------------------------------------------------------------- */

/* This structure represents a module inside the system. */
struct RedisModule
{
    void *handle;  /* Module dlopen() handle. */
    char *name;    /* Module name. */
    int ver;       /* Module version. We use just progressive integers. */
    int apiver;    /* Module API version as requested during initialization.*/
    list *types;   /* Module data types. */
    list *usedby;  /* List of modules using APIs from this one. */
    list *using;   /* List of modules we use some APIs of. */
    list *filters; /* List of filters the module has registered. */
    int in_call;   /* RM_Call() nesting level */
};
typedef struct RedisModule RedisModule;

/* This represents a shared API. Shared APIs will be used to populate
 * the server.sharedapi dictionary, mapping names of APIs exported by
 * modules for other modules to use, to their structure specifying the
 * function pointer that can be called. */
struct RedisModuleSharedAPI
{
    void *func;
    RedisModule *module;
};
typedef struct RedisModuleSharedAPI RedisModuleSharedAPI;

static dict *modules; /* Hash table of modules. SDS -> RedisModule ptr.*/

/* Entries in the context->amqueue array, representing objects to free
 * when the callback returns. */
struct AutoMemEntry
{
    void *ptr;
    int type;
};

/* AutMemEntry type field values. */
#define REDISMODULE_AM_KEY 0
#define REDISMODULE_AM_STRING 1
#define REDISMODULE_AM_REPLY 2
#define REDISMODULE_AM_FREED 3 /* Explicitly freed by user already. */
#define REDISMODULE_AM_DICT 4

/* The pool allocator block. Redis Modules can allocate memory via this special
 * allocator that will automatically release it all once the callback returns.
 * This means that it can only be used for ephemeral allocations. However
 * there are two advantages for modules to use this API:
 *
 * 1) The memory is automatically released when the callback returns.
 * 2) This allocator is faster for many small allocations since whole blocks
 *    are allocated, and small pieces returned to the caller just advancing
 *    the index of the allocation.
 *
 * Allocations are always rounded to the size of the void pointer in order
 * to always return aligned memory chunks. */

#define REDISMODULE_POOL_ALLOC_MIN_SIZE (1024 * 8)
#define REDISMODULE_POOL_ALLOC_ALIGN (sizeof(void *))

typedef struct RedisModulePoolAllocBlock
{
    uint32_t size;
    uint32_t used;
    struct RedisModulePoolAllocBlock *next;
    char memory[];
} RedisModulePoolAllocBlock;

/* This structure represents the context in which Redis modules operate.
 * Most APIs module can access, get a pointer to the context, so that the API
 * implementation can hold state across calls, or remember what to free after
 * the call and so forth.
 *
 * Note that not all the context structure is always filled with actual values
 * but only the fields needed in a given context. */

struct RedisModuleBlockedClient;

struct RedisModuleCtx
{
    void *getapifuncptr;                             /* NOTE: Must be the first field. */
    struct RedisModule *module;                      /* Module reference. */
    client *client;                                  /* Client calling a command. */
    struct RedisModuleBlockedClient *blocked_client; /* Blocked client for
                                                        thread safe context. */
    struct AutoMemEntry *amqueue;                    /* Auto memory queue of objects to free. */
    int amqueue_len;                                 /* Number of slots in amqueue. */
    int amqueue_used;                                /* Number of used slots in amqueue. */
    int flags;                                       /* REDISMODULE_CTX_... flags. */
    void **postponed_arrays;                         /* To set with RM_ReplySetArrayLength(). */
    int postponed_arrays_count;                      /* Number of entries in postponed_arrays. */
    void *blocked_privdata;                          /* Privdata set when unblocking a client. */

    /* Used if there is the REDISMODULE_CTX_KEYS_POS_REQUEST flag set. */
    int *keys_pos;
    int keys_count;

    struct RedisModulePoolAllocBlock *pa_head;
    redisOpArray saved_oparray; /* When propagating commands in a callback
                                   we reallocate the "also propagate" op
                                   array. Here we save the old one to
                                   restore it later. */
};
typedef struct RedisModuleCtx RedisModuleCtx;

#define REDISMODULE_CTX_INIT                                                                                    \
    {                                                                                                           \
        (void *)(unsigned long)&RM_GetApi, NULL, NULL, NULL, NULL, 0, 0, 0, NULL, 0, NULL, NULL, 0, NULL, { 0 } \
    }
#define REDISMODULE_CTX_MULTI_EMITTED (1 << 0)
#define REDISMODULE_CTX_AUTO_MEMORY (1 << 1)
#define REDISMODULE_CTX_KEYS_POS_REQUEST (1 << 2)
#define REDISMODULE_CTX_BLOCKED_REPLY (1 << 3)
#define REDISMODULE_CTX_BLOCKED_TIMEOUT (1 << 4)
#define REDISMODULE_CTX_THREAD_SAFE (1 << 5)
#define REDISMODULE_CTX_BLOCKED_DISCONNECTED (1 << 6)
#define REDISMODULE_CTX_MODULE_COMMAND_CALL (1 << 7)

/* This represents a Redis key opened with RM_OpenKey(). */
struct RedisModuleKey
{
    RedisModuleCtx *ctx;
    redisDb *db;
    robj *key;   /* Key name object. */
    robj *value; /* Value object, or NULL if the key was not found. */
    void *iter;  /* Iterator. */
    int mode;    /* Opening mode. */

    /* Zset iterator. */
    uint32_t ztype;     /* REDISMODULE_ZSET_RANGE_* */
    zrangespec zrs;     /* Score range. */
    zlexrangespec zlrs; /* Lex range. */
    uint32_t zstart;    /* Start pos for positional ranges. */
    uint32_t zend;      /* End pos for positional ranges. */
    void *zcurrent;     /* Zset iterator current node. */
    int zer;            /* Zset iterator end reached flag
                           (true if end was reached). */
};
typedef struct RedisModuleKey RedisModuleKey;

/* RedisModuleKey 'ztype' values. */
#define REDISMODULE_ZSET_RANGE_NONE 0 /* This must always be 0. */
#define REDISMODULE_ZSET_RANGE_LEX 1
#define REDISMODULE_ZSET_RANGE_SCORE 2
#define REDISMODULE_ZSET_RANGE_POS 3

/* Function pointer type of a function representing a command inside
 * a Redis module. */
struct RedisModuleBlockedClient;
typedef int (*RedisModuleCmdFunc)(RedisModuleCtx *ctx, void **argv, int argc);
typedef void (*RedisModuleDisconnectFunc)(RedisModuleCtx *ctx, struct RedisModuleBlockedClient *bc);

/* This struct holds the information about a command registered by a module.*/
struct RedisModuleCommandProxy
{
    struct RedisModule *module;
    RedisModuleCmdFunc func;
    struct redisCommand *rediscmd;
};
typedef struct RedisModuleCommandProxy RedisModuleCommandProxy;

#define REDISMODULE_REPLYFLAG_NONE 0
#define REDISMODULE_REPLYFLAG_TOPARSE (1 << 0) /* Protocol must be parsed. */
#define REDISMODULE_REPLYFLAG_NESTED (1 << 1)  /* Nested reply object. No proto \
                                                  or struct free. */

/* Reply of RM_Call() function. The function is filled in a lazy
 * way depending on the function called on the reply structure. By default
 * only the type, proto and protolen are filled. */
typedef struct RedisModuleCallReply
{
    RedisModuleCtx *ctx;
    int type;        /* REDISMODULE_REPLY_... */
    int flags;       /* REDISMODULE_REPLYFLAG_...  */
    size_t len;      /* Len of strings or num of elements of arrays. */
    char *proto;     /* Raw reply protocol. An SDS string at top-level object. */
    size_t protolen; /* Length of protocol. */
    union
    {
        const char *str;                    /* String pointer for string and error replies. This
                                               does not need to be freed, always points inside
                                               a reply->proto buffer of the reply object or, in
                                               case of array elements, of parent reply objects. */
        long long ll;                       /* Reply value for integer reply. */
        struct RedisModuleCallReply *array; /* Array of sub-reply elements. */
    } val;
} RedisModuleCallReply;

/* Structure representing a blocked client. We get a pointer to such
 * an object when blocking from modules. */
typedef struct RedisModuleBlockedClient
{
    client *client;                                  /* Pointer to the blocked client. or NULL if the client
                                                        was destroyed during the life of this object. */
    RedisModule *module;                             /* Module blocking the client. */
    RedisModuleCmdFunc reply_callback;               /* Reply callback on normal completion.*/
    RedisModuleCmdFunc timeout_callback;             /* Reply callback on timeout. */
    RedisModuleDisconnectFunc disconnect_callback;   /* Called on disconnection.*/
    void (*free_privdata)(RedisModuleCtx *, void *); /* privdata cleanup callback.*/
    void *privdata;                                  /* Module private data that may be used by the reply
                                                        or timeout callback. It is set via the
                                                        RedisModule_UnblockClient() API. */
    client *reply_client;                            /* Fake client used to accumulate replies
                                                        in thread safe contexts. */
    int dbid;                                        /* Database number selected by the original client. */
} RedisModuleBlockedClient;

static pthread_mutex_t moduleUnblockedClientsMutex = PTHREAD_MUTEX_INITIALIZER;
static list *moduleUnblockedClients;

/* We need a mutex that is unlocked / relocked in beforeSleep() in order to
 * allow thread safe contexts to execute commands at a safe moment. */
static pthread_mutex_t moduleGIL = PTHREAD_MUTEX_INITIALIZER;

/* Function pointer type for keyspace event notification subscriptions from modules. */
typedef int (*RedisModuleNotificationFunc)(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key);

/* Keyspace notification subscriber information.
 * See RM_SubscribeToKeyspaceEvents() for more information. */
typedef struct RedisModuleKeyspaceSubscriber
{
    /* The module subscribed to the event */
    RedisModule *module;
    /* Notification callback in the module*/
    RedisModuleNotificationFunc notify_callback;
    /* A bit mask of the events the module is interested in */
    int event_mask;
    /* Active flag set on entry, to avoid reentrant subscribers
     * calling themselves */
    int active;
} RedisModuleKeyspaceSubscriber;

/* The module keyspace notification subscribers list */
static list *moduleKeyspaceSubscribers;

/* Static client recycled for when we need to provide a context with a client
 * in a situation where there is no client to provide. This avoidsallocating
 * a new client per round. For instance this is used in the keyspace
 * notifications, timers and cluster messages callbacks. */
static client *moduleFreeContextReusedClient;

/* Data structures related to the exported dictionary data structure. */
typedef struct RedisModuleDict
{
    rax *rax; /* The radix tree. */
} RedisModuleDict;

typedef struct RedisModuleDictIter
{
    RedisModuleDict *dict;
    raxIterator ri;
} RedisModuleDictIter;

typedef struct RedisModuleCommandFilterCtx
{
    RedisModuleString **argv;
    int argc;
} RedisModuleCommandFilterCtx;

typedef void (*RedisModuleCommandFilterFunc)(RedisModuleCommandFilterCtx *filter);

typedef struct RedisModuleCommandFilter
{
    /* The module that registered the filter */
    RedisModule *module;
    /* Filter callback function */
    RedisModuleCommandFilterFunc callback;
    /* REDISMODULE_CMDFILTER_* flags */
    int flags;
} RedisModuleCommandFilter;

/* Registered filters */
static list *moduleCommandFilters;

/* Flags for moduleCreateArgvFromUserFormat(). */
#define REDISMODULE_ARGV_REPLICATE (1 << 0)
#define REDISMODULE_ARGV_NO_AOF (1 << 1)
#define REDISMODULE_ARGV_NO_REPLICAS (1 << 2)

/* --------------------------------------------------------------------------
 * Prototypes
 * -------------------------------------------------------------------------- */

void RM_FreeCallReply(RedisModuleCallReply *reply);
void RM_CloseKey(RedisModuleKey *key);
void autoMemoryCollect(RedisModuleCtx *ctx);
robj **moduleCreateArgvFromUserFormat(const char *cmdname, const char *fmt, int *argcp, int *flags, va_list ap);
void moduleReplicateMultiIfNeeded(RedisModuleCtx *ctx);
void RM_ZsetRangeStop(RedisModuleKey *kp);
static void zsetKeyReset(RedisModuleKey *key);
void RM_FreeDict(RedisModuleCtx *ctx, RedisModuleDict *d);

/* --------------------------------------------------------------------------
 * Heap allocation raw functions
 * -------------------------------------------------------------------------- */

/* Use like malloc(). Memory allocated with this function is reported in
 * Redis INFO memory, used for keys eviction according to maxmemory settings
 * and in general is taken into account as memory allocated by Redis.
 * You should avoid using malloc(). */
void *RM_Alloc(size_t bytes)
{
    return zmalloc(bytes);
}

/* Use like calloc(). Memory allocated with this function is reported in
 * Redis INFO memory, used for keys eviction according to maxmemory settings
 * and in general is taken into account as memory allocated by Redis.
 * You should avoid using calloc() directly. */
void *RM_Calloc(size_t nmemb, size_t size)
{
    return zcalloc(nmemb * size);
}

/* Use like realloc() for memory obtained with RedisModule_Alloc(). */
void *RM_Realloc(void *ptr, size_t bytes)
{
    return zrealloc(ptr, bytes);
}

/* Use like free() for memory obtained by RedisModule_Alloc() and
 * RedisModule_Realloc(). However you should never try to free with
 * RedisModule_Free() memory allocated with malloc() inside your module. */
void RM_Free(void *ptr)
{
    zfree(ptr);
}

/* Like strdup() but returns memory allocated with RedisModule_Alloc(). */
char *RM_Strdup(const char *str)
{
    return zstrdup(str);
}

/* --------------------------------------------------------------------------
 * Pool allocator
 * -------------------------------------------------------------------------- */

/* Release the chain of blocks used for pool allocations. */
void poolAllocRelease(RedisModuleCtx *ctx)
{
    RedisModulePoolAllocBlock *head = ctx->pa_head, *next;

    while (head != NULL)
    {
        next = head->next;
        zfree(head);
        head = next;
    }
    ctx->pa_head = NULL;
}

/* Return heap allocated memory that will be freed automatically when the
 * module callback function returns. Mostly suitable for small allocations
 * that are short living and must be released when the callback returns
 * anyway. The returned memory is aligned to the architecture word size
 * if at least word size bytes are requested, otherwise it is just
 * aligned to the next power of two, so for example a 3 bytes request is
 * 4 bytes aligned while a 2 bytes request is 2 bytes aligned.
 *
 * There is no realloc style function since when this is needed to use the
 * pool allocator is not a good idea.
 *
 * The function returns NULL if `bytes` is 0. */
void *RM_PoolAlloc(RedisModuleCtx *ctx, size_t bytes)
{
    if (bytes == 0)
        return NULL;
    RedisModulePoolAllocBlock *b = ctx->pa_head;
    size_t left = b ? b->size - b->used : 0;

    /* Fix alignment. */
    if (left >= bytes)
    {
        size_t alignment = REDISMODULE_POOL_ALLOC_ALIGN;
        while (bytes < alignment && alignment / 2 >= bytes)
            alignment /= 2;
        if (b->used % alignment)
            b->used += alignment - (b->used % alignment);
        left = (b->used > b->size) ? 0 : b->size - b->used;
    }

    /* Create a new block if needed. */
    if (left < bytes)
    {
        size_t blocksize = REDISMODULE_POOL_ALLOC_MIN_SIZE;
        if (blocksize < bytes)
            blocksize = bytes;
        b = zmalloc(sizeof(*b) + blocksize);
        b->size = blocksize;
        b->used = 0;
        b->next = ctx->pa_head;
        ctx->pa_head = b;
    }

    char *retval = b->memory + b->used;
    b->used += bytes;
    return retval;
}

/* --------------------------------------------------------------------------
 * Helpers for modules API implementation
 * -------------------------------------------------------------------------- */

/* Create an empty key of the specified type. 'kp' must point to a key object
 * opened for writing where the .value member is set to NULL because the
 * key was found to be non existing.
 *
 * On success REDISMODULE_OK is returned and the key is populated with
 * the value of the specified type. The function fails and returns
 * REDISMODULE_ERR if:
 *
 * 1) The key is not open for writing.
 * 2) The key is not empty.
 * 3) The specified type is unknown.
 */
int moduleCreateEmptyKey(RedisModuleKey *key, int type)
{
    robj *obj;

    /* The key must be open for writing and non existing to proceed. */
    if (!(key->mode & REDISMODULE_WRITE) || key->value)
        return REDISMODULE_ERR;

    switch (type)
    {
    case REDISMODULE_KEYTYPE_LIST:
        obj = createQuicklistObject();
        quicklistSetOptions(obj->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);
        break;
    case REDISMODULE_KEYTYPE_ZSET:
        obj = createZsetZiplistObject();
        break;
    case REDISMODULE_KEYTYPE_HASH:
        obj = createHashObject();
        break;
    default:
        return REDISMODULE_ERR;
    }
    dbAdd(key->db, key->key, obj);
    key->value = obj;
    return REDISMODULE_OK;
}

/* This function is called in low-level API implementation functions in order
 * to check if the value associated with the key remained empty after an
 * operation that removed elements from an aggregate data type.
 *
 * If this happens, the key is deleted from the DB and the key object state
 * is set to the right one in order to be targeted again by write operations
 * possibly recreating the key if needed.
 *
 * The function returns 1 if the key value object is found empty and is
 * deleted, otherwise 0 is returned. */
int moduleDelKeyIfEmpty(RedisModuleKey *key)
{
    if (!(key->mode & REDISMODULE_WRITE) || key->value == NULL)
        return 0;
    int isempty;
    robj *o = key->value;

    switch (o->type)
    {
    case OBJ_LIST:
        isempty = listTypeLength(o) == 0;
        break;
    case OBJ_SET:
        isempty = setTypeSize(o) == 0;
        break;
    case OBJ_ZSET:
        isempty = zsetLength(o) == 0;
        break;
    case OBJ_HASH:
        isempty = hashTypeLength(o) == 0;
        break;
    default:
        isempty = 0;
    }

    if (isempty)
    {
        dbDelete(key->db, key->key);
        key->value = NULL;
        return 1;
    }
    else
    {
        return 0;
    }
}

/* --------------------------------------------------------------------------
 * Service API exported to modules
 *
 * Note that all the exported APIs are called RM_<funcname> in the core
 * and RedisModule_<funcname> in the module side (defined as function
 * pointers in redismodule.h). In this way the dynamic linker does not
 * mess with our global function pointers, overriding it with the symbols
 * defined in the main executable having the same names.
 * -------------------------------------------------------------------------- */

/* Lookup the requested module API and store the function pointer into the
 * target pointer. The function returns REDISMODULE_ERR if there is no such
 * named API, otherwise REDISMODULE_OK.
 *
 * This function is not meant to be used by modules developer, it is only
 * used implicitly by including redismodule.h. */
int RM_GetApi(const char *funcname, void **targetPtrPtr)
{
    dictEntry *he = dictFind(server.moduleapi, funcname);
    if (!he)
        return REDISMODULE_ERR;
    *targetPtrPtr = dictGetVal(he);
    return REDISMODULE_OK;
}

/* Helper function for when a command callback is called, in order to handle
 * details needed to correctly replicate commands. */
void moduleHandlePropagationAfterCommandCallback(RedisModuleCtx *ctx)
{
    client *c = ctx->client;

    /* We don't need to do anything here if the context was never used
     * in order to propagate commands. */
    if (!(ctx->flags & REDISMODULE_CTX_MULTI_EMITTED))
        return;

    if (c->flags & CLIENT_LUA)
        return;

    /* Handle the replication of the final EXEC, since whatever a command
     * emits is always wrapped around MULTI/EXEC. */
    robj *propargv[1];
    propargv[0] = createStringObject("EXEC", 4);
    alsoPropagate(server.execCommand, c->db->id, propargv, 1,
                  PROPAGATE_AOF | PROPAGATE_REPL);
    decrRefCount(propargv[0]);

    /* If this is not a module command context (but is instead a simple
     * callback context), we have to handle directly the "also propagate"
     * array and emit it. In a module command call this will be handled
     * directly by call(). */
    if (!(ctx->flags & REDISMODULE_CTX_MODULE_COMMAND_CALL) &&
        server.also_propagate.numops)
    {
        for (int j = 0; j < server.also_propagate.numops; j++)
        {
            redisOp *rop = &server.also_propagate.ops[j];
            int target = rop->target;
            if (target)
                propagate(rop->cmd, rop->dbid, rop->argv, rop->argc, target);
        }
        redisOpArrayFree(&server.also_propagate);
        /* Restore the previous oparray in case of nexted use of the API. */
        server.also_propagate = ctx->saved_oparray;
    }
}

/* Free the context after the user function was called. */
void moduleFreeContext(RedisModuleCtx *ctx)
{
    moduleHandlePropagationAfterCommandCallback(ctx);
    autoMemoryCollect(ctx);
    poolAllocRelease(ctx);
    if (ctx->postponed_arrays)
    {
        zfree(ctx->postponed_arrays);
        ctx->postponed_arrays_count = 0;
        serverLog(LL_WARNING,
                  "API misuse detected in module %s: "
                  "RedisModule_ReplyWithArray(REDISMODULE_POSTPONED_ARRAY_LEN) "
                  "not matched by the same number of RedisModule_SetReplyArrayLen() "
                  "calls.",
                  ctx->module->name);
    }
    if (ctx->flags & REDISMODULE_CTX_THREAD_SAFE)
        freeClient(ctx->client);
}

/* This Redis command binds the normal Redis command invocation with commands
 * exported by modules. */
void RedisModuleCommandDispatcher(client *c)
{
    RedisModuleCommandProxy *cp = (void *)(unsigned long)c->cmd->getkeys_proc;
    RedisModuleCtx ctx = REDISMODULE_CTX_INIT;

    ctx.flags |= REDISMODULE_CTX_MODULE_COMMAND_CALL;
    ctx.module = cp->module;
    ctx.client = c;
    cp->func(&ctx, (void **)c->argv, c->argc);
    moduleFreeContext(&ctx);

    /* In some cases processMultibulkBuffer uses sdsMakeRoomFor to
     * expand the query buffer, and in order to avoid a big object copy
     * the query buffer SDS may be used directly as the SDS string backing
     * the client argument vectors: sometimes this will result in the SDS
     * string having unused space at the end. Later if a module takes ownership
     * of the RedisString, such space will be wasted forever. Inside the
     * Redis core this is not a problem because tryObjectEncoding() is called
     * before storing strings in the key space. Here we need to do it
     * for the module. */
    for (int i = 0; i < c->argc; i++)
    {
        /* Only do the work if the module took ownership of the object:
         * in that case the refcount is no longer 1. */
        if (c->argv[i]->refcount > 1)
            trimStringObjectIfNeeded(c->argv[i]);
    }
}

/* This function returns the list of keys, with the same interface as the
 * 'getkeys' function of the native commands, for module commands that exported
 * the "getkeys-api" flag during the registration. This is done when the
 * list of keys are not at fixed positions, so that first/last/step cannot
 * be used.
 *
 * In order to accomplish its work, the module command is called, flagging
 * the context in a way that the command can recognize this is a special
 * "get keys" call by calling RedisModule_IsKeysPositionRequest(ctx). */
int *moduleGetCommandKeysViaAPI(struct redisCommand *cmd, robj **argv, int argc, int *numkeys)
{
    RedisModuleCommandProxy *cp = (void *)(unsigned long)cmd->getkeys_proc;
    RedisModuleCtx ctx = REDISMODULE_CTX_INIT;

    ctx.module = cp->module;
    ctx.client = NULL;
    ctx.flags |= REDISMODULE_CTX_KEYS_POS_REQUEST;
    cp->func(&ctx, (void **)argv, argc);
    int *res = ctx.keys_pos;
    if (numkeys)
        *numkeys = ctx.keys_count;
    moduleFreeContext(&ctx);
    return res;
}

/* Return non-zero if a module command, that was declared with the
 * flag "getkeys-api", is called in a special way to get the keys positions
 * and not to get executed. Otherwise zero is returned. */
int RM_IsKeysPositionRequest(RedisModuleCtx *ctx)
{
    return (ctx->flags & REDISMODULE_CTX_KEYS_POS_REQUEST) != 0;
}

/* When a module command is called in order to obtain the position of
 * keys, since it was flagged as "getkeys-api" during the registration,
 * the command implementation checks for this special call using the
 * RedisModule_IsKeysPositionRequest() API and uses this function in
 * order to report keys, like in the following example:
 *
 *     if (RedisModule_IsKeysPositionRequest(ctx)) {
 *         RedisModule_KeyAtPos(ctx,1);
 *         RedisModule_KeyAtPos(ctx,2);
 *     }
 *
 *  Note: in the example below the get keys API would not be needed since
 *  keys are at fixed positions. This interface is only used for commands
 *  with a more complex structure. */
void RM_KeyAtPos(RedisModuleCtx *ctx, int pos)
{
    if (!(ctx->flags & REDISMODULE_CTX_KEYS_POS_REQUEST))
        return;
    if (pos <= 0)
        return;
    ctx->keys_pos = zrealloc(ctx->keys_pos, sizeof(int) * (ctx->keys_count + 1));
    ctx->keys_pos[ctx->keys_count++] = pos;
}

/* Helper for RM_CreateCommand(). Turns a string representing command
 * flags into the command flags used by the Redis core.
 *
 * It returns the set of flags, or -1 if unknown flags are found. */
int64_t commandFlagsFromString(char *s)
{
    int count, j;
    int64_t flags = 0;
    sds *tokens = sdssplitlen(s, strlen(s), " ", 1, &count);
    for (j = 0; j < count; j++)
    {
        char *t = tokens[j];
        if (!strcasecmp(t, "write"))
            flags |= CMD_WRITE;
        else if (!strcasecmp(t, "readonly"))
            flags |= CMD_READONLY;
        else if (!strcasecmp(t, "admin"))
            flags |= CMD_ADMIN;
        else if (!strcasecmp(t, "deny-oom"))
            flags |= CMD_DENYOOM;
        else if (!strcasecmp(t, "deny-script"))
            flags |= CMD_NOSCRIPT;
        else if (!strcasecmp(t, "allow-loading"))
            flags |= CMD_LOADING;
        else if (!strcasecmp(t, "pubsub"))
            flags |= CMD_PUBSUB;
        else if (!strcasecmp(t, "random"))
            flags |= CMD_RANDOM;
        else if (!strcasecmp(t, "allow-stale"))
            flags |= CMD_STALE;
        else if (!strcasecmp(t, "no-monitor"))
            flags |= CMD_SKIP_MONITOR;
        else if (!strcasecmp(t, "fast"))
            flags |= CMD_FAST;
        else if (!strcasecmp(t, "getkeys-api"))
            flags |= CMD_MODULE_GETKEYS;
        else if (!strcasecmp(t, "no-cluster"))
            flags |= CMD_MODULE_NO_CLUSTER;
        else
            break;
    }
    sdsfreesplitres(tokens, count);
    if (j != count)
        return -1; /* Some token not processed correctly. */
    return flags;
}

/* Register a new command in the Redis server, that will be handled by
 * calling the function pointer 'func' using the RedisModule calling
 * convention. The function returns REDISMODULE_ERR if the specified command
 * name is already busy or a set of invalid flags were passed, otherwise
 * REDISMODULE_OK is returned and the new command is registered.
 *
 * This function must be called during the initialization of the module
 * inside the RedisModule_OnLoad() function. Calling this function outside
 * of the initialization function is not defined.
 *
 * The command function type is the following:
 *
 *      int MyCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
 *
 * And is supposed to always return REDISMODULE_OK.
 *
 * The set of flags 'strflags' specify the behavior of the command, and should
 * be passed as a C string composed of space separated words, like for
 * example "write deny-oom". The set of flags are:
 *
 * * **"write"**:     The command may modify the data set (it may also read
 *                    from it).
 * * **"readonly"**:  The command returns data from keys but never writes.
 * * **"admin"**:     The command is an administrative command (may change
 *                    replication or perform similar tasks).
 * * **"deny-oom"**:  The command may use additional memory and should be
 *                    denied during out of memory conditions.
 * * **"deny-script"**:   Don't allow this command in Lua scripts.
 * * **"allow-loading"**: Allow this command while the server is loading data.
 *                        Only commands not interacting with the data set
 *                        should be allowed to run in this mode. If not sure
 *                        don't use this flag.
 * * **"pubsub"**:    The command publishes things on Pub/Sub channels.
 * * **"random"**:    The command may have different outputs even starting
 *                    from the same input arguments and key values.
 * * **"allow-stale"**: The command is allowed to run on slaves that don't
 *                      serve stale data. Don't use if you don't know what
 *                      this means.
 * * **"no-monitor"**: Don't propagate the command on monitor. Use this if
 *                     the command has sensible data among the arguments.
 * * **"fast"**:      The command time complexity is not greater
 *                    than O(log(N)) where N is the size of the collection or
 *                    anything else representing the normal scalability
 *                    issue with the command.
 * * **"getkeys-api"**: The command implements the interface to return
 *                      the arguments that are keys. Used when start/stop/step
 *                      is not enough because of the command syntax.
 * * **"no-cluster"**: The command should not register in Redis Cluster
 *                     since is not designed to work with it because, for
 *                     example, is unable to report the position of the
 *                     keys, programmatically creates key names, or any
 *                     other reason.
 */
int RM_CreateCommand(RedisModuleCtx *ctx, const char *name, RedisModuleCmdFunc cmdfunc, const char *strflags, int firstkey, int lastkey, int keystep)
{
    int64_t flags = strflags ? commandFlagsFromString((char *)strflags) : 0;
    if (flags == -1)
        return REDISMODULE_ERR;
    if ((flags & CMD_MODULE_NO_CLUSTER) && server.cluster_enabled)
        return REDISMODULE_ERR;

    struct redisCommand *rediscmd;
    RedisModuleCommandProxy *cp;
    sds cmdname = sdsnew(name);

    /* Check if the command name is busy. */
    if (lookupCommand(cmdname) != NULL)
    {
        sdsfree(cmdname);
        return REDISMODULE_ERR;
    }

    /* Create a command "proxy", which is a structure that is referenced
     * in the command table, so that the generic command that works as
     * binding between modules and Redis, can know what function to call
     * and what the module is.
     *
     * Note that we use the Redis command table 'getkeys_proc' in order to
     * pass a reference to the command proxy structure. */
    cp = zmalloc(sizeof(*cp));
    cp->module = ctx->module;
    cp->func = cmdfunc;
    cp->rediscmd = zmalloc(sizeof(*rediscmd));
    cp->rediscmd->name = cmdname;
    cp->rediscmd->proc = RedisModuleCommandDispatcher;
    cp->rediscmd->arity = -1;
    cp->rediscmd->flags = flags | CMD_MODULE;
    cp->rediscmd->getkeys_proc = (redisGetKeysProc *)(unsigned long)cp;
    cp->rediscmd->firstkey = firstkey;
    cp->rediscmd->lastkey = lastkey;
    cp->rediscmd->keystep = keystep;
    cp->rediscmd->microseconds = 0;
    cp->rediscmd->calls = 0;
    dictAdd(server.commands, sdsdup(cmdname), cp->rediscmd);
    dictAdd(server.orig_commands, sdsdup(cmdname), cp->rediscmd);
    return REDISMODULE_OK;
}

/* Called by RM_Init() to setup the `ctx->module` structure.
 *
 * This is an internal function, Redis modules developers don't need
 * to use it. */
void RM_SetModuleAttribs(RedisModuleCtx *ctx, const char *name, int ver, int apiver)
{
    RedisModule *module;

    if (ctx->module != NULL)
        return;
    module = zmalloc(sizeof(*module));
    module->name = sdsnew((char *)name);
    module->ver = ver;
    module->apiver = apiver;
    module->types = listCreate();
    module->usedby = listCreate();
    module->using = listCreate();
    module->filters = listCreate();
    module->in_call = 0;
    ctx->module = module;
}

/* Return non-zero if the module name is busy.
 * Otherwise zero is returned. */
int RM_IsModuleNameBusy(const char *name)
{
    sds modulename = sdsnew(name);
    dictEntry *de = dictFind(modules, modulename);
    sdsfree(modulename);
    return de != NULL;
}

/* Return the current UNIX time in milliseconds. */
long long RM_Milliseconds(void)
{
    return mstime();
}

/* --------------------------------------------------------------------------
 * Automatic memory management for modules
 * -------------------------------------------------------------------------- */

/* Enable automatic memory management. See API.md for more information.
 *
 * The function must be called as the first function of a command implementation
 * that wants to use automatic memory. */
void RM_AutoMemory(RedisModuleCtx *ctx)
{
    ctx->flags |= REDISMODULE_CTX_AUTO_MEMORY;
}

/* Add a new object to release automatically when the callback returns. */
void autoMemoryAdd(RedisModuleCtx *ctx, int type, void *ptr)
{
    if (!(ctx->flags & REDISMODULE_CTX_AUTO_MEMORY))
        return;
    if (ctx->amqueue_used == ctx->amqueue_len)
    {
        ctx->amqueue_len *= 2;
        if (ctx->amqueue_len < 16)
            ctx->amqueue_len = 16;
        ctx->amqueue = zrealloc(ctx->amqueue, sizeof(struct AutoMemEntry) * ctx->amqueue_len);
    }
    ctx->amqueue[ctx->amqueue_used].type = type;
    ctx->amqueue[ctx->amqueue_used].ptr = ptr;
    ctx->amqueue_used++;
}

/* Mark an object as freed in the auto release queue, so that users can still
 * free things manually if they want.
 *
 * The function returns 1 if the object was actually found in the auto memory
 * pool, otherwise 0 is returned. */
int autoMemoryFreed(RedisModuleCtx *ctx, int type, void *ptr)
{
    if (!(ctx->flags & REDISMODULE_CTX_AUTO_MEMORY))
        return 0;

    int count = (ctx->amqueue_used + 1) / 2;
    for (int j = 0; j < count; j++)
    {
        for (int side = 0; side < 2; side++)
        {
            /* For side = 0 check right side of the array, for
             * side = 1 check the left side instead (zig-zag scanning). */
            int i = (side == 0) ? (ctx->amqueue_used - 1 - j) : j;
            if (ctx->amqueue[i].type == type &&
                ctx->amqueue[i].ptr == ptr)
            {
                ctx->amqueue[i].type = REDISMODULE_AM_FREED;

                /* Switch the freed element and the last element, to avoid growing
                 * the queue unnecessarily if we allocate/free in a loop */
                if (i != ctx->amqueue_used - 1)
                {
                    ctx->amqueue[i] = ctx->amqueue[ctx->amqueue_used - 1];
                }

                /* Reduce the size of the queue because we either moved the top
                 * element elsewhere or freed it */
                ctx->amqueue_used--;
                return 1;
            }
        }
    }
    return 0;
}

/* Release all the objects in queue. */
void autoMemoryCollect(RedisModuleCtx *ctx)
{
    if (!(ctx->flags & REDISMODULE_CTX_AUTO_MEMORY))
        return;
    /* Clear the AUTO_MEMORY flag from the context, otherwise the functions
     * we call to free the resources, will try to scan the auto release
     * queue to mark the entries as freed. */
    ctx->flags &= ~REDISMODULE_CTX_AUTO_MEMORY;
    int j;
    for (j = 0; j < ctx->amqueue_used; j++)
    {
        void *ptr = ctx->amqueue[j].ptr;
        switch (ctx->amqueue[j].type)
        {
        case REDISMODULE_AM_STRING:
            decrRefCount(ptr);
            break;
        case REDISMODULE_AM_REPLY:
            RM_FreeCallReply(ptr);
            break;
        case REDISMODULE_AM_KEY:
            RM_CloseKey(ptr);
            break;
        case REDISMODULE_AM_DICT:
            RM_FreeDict(NULL, ptr);
            break;
        }
    }
    ctx->flags |= REDISMODULE_CTX_AUTO_MEMORY;
    zfree(ctx->amqueue);
    ctx->amqueue = NULL;
    ctx->amqueue_len = 0;
    ctx->amqueue_used = 0;
}

/* --------------------------------------------------------------------------
 * String objects APIs
 * -------------------------------------------------------------------------- */

/* Create a new module string object. The returned string must be freed
 * with RedisModule_FreeString(), unless automatic memory is enabled.
 *
 * The string is created by copying the `len` bytes starting
 * at `ptr`. No reference is retained to the passed buffer.
 *
 * The module context 'ctx' is optional and may be NULL if you want to create
 * a string out of the context scope. However in that case, the automatic
 * memory management will not be available, and the string memory must be
 * managed manually. */
RedisModuleString *RM_CreateString(RedisModuleCtx *ctx, const char *ptr, size_t len)
{
    RedisModuleString *o = createStringObject(ptr, len);
    if (ctx != NULL)
        autoMemoryAdd(ctx, REDISMODULE_AM_STRING, o);
    return o;
}

/* Create a new module string object from a printf format and arguments.
 * The returned string must be freed with RedisModule_FreeString(), unless
 * automatic memory is enabled.
 *
 * The string is created using the sds formatter function sdscatvprintf().
 *
 * The passed context 'ctx' may be NULL if necessary, see the
 * RedisModule_CreateString() documentation for more info. */
RedisModuleString *RM_CreateStringPrintf(RedisModuleCtx *ctx, const char *fmt, ...)
{
    sds s = sdsempty();

    va_list ap;
    va_start(ap, fmt);
    s = sdscatvprintf(s, fmt, ap);
    va_end(ap);

    RedisModuleString *o = createObject(OBJ_STRING, s);
    if (ctx != NULL)
        autoMemoryAdd(ctx, REDISMODULE_AM_STRING, o);

    return o;
}

/* Like RedisModule_CreatString(), but creates a string starting from a long long
 * integer instead of taking a buffer and its length.
 *
 * The returned string must be released with RedisModule_FreeString() or by
 * enabling automatic memory management.
 *
 * The passed context 'ctx' may be NULL if necessary, see the
 * RedisModule_CreateString() documentation for more info. */
RedisModuleString *RM_CreateStringFromLongLong(RedisModuleCtx *ctx, long long ll)
{
    char buf[LONG_STR_SIZE];
    size_t len = ll2string(buf, sizeof(buf), ll);
    return RM_CreateString(ctx, buf, len);
}

/* Like RedisModule_CreatString(), but creates a string starting from another
 * RedisModuleString.
 *
 * The returned string must be released with RedisModule_FreeString() or by
 * enabling automatic memory management.
 *
 * The passed context 'ctx' may be NULL if necessary, see the
 * RedisModule_CreateString() documentation for more info. */
RedisModuleString *RM_CreateStringFromString(RedisModuleCtx *ctx, const RedisModuleString *str)
{
    RedisModuleString *o = dupStringObject(str);
    if (ctx != NULL)
        autoMemoryAdd(ctx, REDISMODULE_AM_STRING, o);
    return o;
}

/* Free a module string object obtained with one of the Redis modules API calls
 * that return new string objects.
 *
 * It is possible to call this function even when automatic memory management
 * is enabled. In that case the string will be released ASAP and removed
 * from the pool of string to release at the end.
 *
 * If the string was created with a NULL context 'ctx', it is also possible to
 * pass ctx as NULL when releasing the string (but passing a context will not
 * create any issue). Strings created with a context should be freed also passing
 * the context, so if you want to free a string out of context later, make sure
 * to create it using a NULL context. */
void RM_FreeString(RedisModuleCtx *ctx, RedisModuleString *str)
{
    decrRefCount(str);
    if (ctx != NULL)
        autoMemoryFreed(ctx, REDISMODULE_AM_STRING, str);
}

/* Every call to this function, will make the string 'str' requiring
 * an additional call to RedisModule_FreeString() in order to really
 * free the string. Note that the automatic freeing of the string obtained
 * enabling modules automatic memory management counts for one
 * RedisModule_FreeString() call (it is just executed automatically).
 *
 * Normally you want to call this function when, at the same time
 * the following conditions are true:
 *
 * 1) You have automatic memory management enabled.
 * 2) You want to create string objects.
 * 3) Those string objects you create need to live *after* the callback
 *    function(for example a command implementation) creating them returns.
 *
 * Usually you want this in order to store the created string object
 * into your own data structure, for example when implementing a new data
 * type.
 *
 * Note that when memory management is turned off, you don't need
 * any call to RetainString() since creating a string will always result
 * into a string that lives after the callback function returns, if
 * no FreeString() call is performed.
 *
 * It is possible to call this function with a NULL context. */
void RM_RetainString(RedisModuleCtx *ctx, RedisModuleString *str)
{
    if (ctx == NULL || !autoMemoryFreed(ctx, REDISMODULE_AM_STRING, str))
    {
        /* Increment the string reference counting only if we can't
         * just remove the object from the list of objects that should
         * be reclaimed. Why we do that, instead of just incrementing
         * the refcount in any case, and let the automatic FreeString()
         * call at the end to bring the refcount back at the desired
         * value? Because this way we ensure that the object refcount
         * value is 1 (instead of going to 2 to be dropped later to 1)
         * after the call to this function. This is needed for functions
         * like RedisModule_StringAppendBuffer() to work. */
        incrRefCount(str);
    }
}

/* Given a string module object, this function returns the string pointer
 * and length of the string. The returned pointer and length should only
 * be used for read only accesses and never modified. */
const char *RM_StringPtrLen(const RedisModuleString *str, size_t *len)
{
    if (str == NULL)
    {
        const char *errmsg = "(NULL string reply referenced in module)";
        if (len)
            *len = strlen(errmsg);
        return errmsg;
    }
    if (len)
        *len = sdslen(str->ptr);
    return str->ptr;
}

/* --------------------------------------------------------------------------
 * Higher level string operations
 * ------------------------------------------------------------------------- */

/* Convert the string into a long long integer, storing it at `*ll`.
 * Returns REDISMODULE_OK on success. If the string can't be parsed
 * as a valid, strict long long (no spaces before/after), REDISMODULE_ERR
 * is returned. */
int RM_StringToLongLong(const RedisModuleString *str, long long *ll)
{
    return string2ll(str->ptr, sdslen(str->ptr), ll) ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Convert the string into a double, storing it at `*d`.
 * Returns REDISMODULE_OK on success or REDISMODULE_ERR if the string is
 * not a valid string representation of a double value. */
int RM_StringToDouble(const RedisModuleString *str, double *d)
{
    int retval = getDoubleFromObject(str, d);
    return (retval == C_OK) ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Compare two string objects, returning -1, 0 or 1 respectively if
 * a < b, a == b, a > b. Strings are compared byte by byte as two
 * binary blobs without any encoding care / collation attempt. */
int RM_StringCompare(RedisModuleString *a, RedisModuleString *b)
{
    return compareStringObjects(a, b);
}

/* Return the (possibly modified in encoding) input 'str' object if
 * the string is unshared, otherwise NULL is returned. */
RedisModuleString *moduleAssertUnsharedString(RedisModuleString *str)
{
    if (str->refcount != 1)
    {
        serverLog(LL_WARNING,
                  "Module attempted to use an in-place string modify operation "
                  "with a string referenced multiple times. Please check the code "
                  "for API usage correctness.");
        return NULL;
    }
    if (str->encoding == OBJ_ENCODING_EMBSTR)
    {
        /* Note: here we "leak" the additional allocation that was
         * used in order to store the embedded string in the object. */
        str->ptr = sdsnewlen(str->ptr, sdslen(str->ptr));
        str->encoding = OBJ_ENCODING_RAW;
    }
    else if (str->encoding == OBJ_ENCODING_INT)
    {
        /* Convert the string from integer to raw encoding. */
        str->ptr = sdsfromlonglong((long)str->ptr);
        str->encoding = OBJ_ENCODING_RAW;
    }
    return str;
}

/* Append the specified buffer to the string 'str'. The string must be a
 * string created by the user that is referenced only a single time, otherwise
 * REDISMODULE_ERR is returned and the operation is not performed. */
int RM_StringAppendBuffer(RedisModuleCtx *ctx, RedisModuleString *str, const char *buf, size_t len)
{
    UNUSED(ctx);
    str = moduleAssertUnsharedString(str);
    if (str == NULL)
        return REDISMODULE_ERR;
    str->ptr = sdscatlen(str->ptr, buf, len);
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Reply APIs
 *
 * Most functions always return REDISMODULE_OK so you can use it with
 * 'return' in order to return from the command implementation with:
 *
 *     if (... some condition ...)
 *         return RM_ReplyWithLongLong(ctx,mycount);
 * -------------------------------------------------------------------------- */

/* Send an error about the number of arguments given to the command,
 * citing the command name in the error message.
 *
 * Example:
 *
 *     if (argc != 3) return RedisModule_WrongArity(ctx);
 */
int RM_WrongArity(RedisModuleCtx *ctx)
{
    addReplyErrorFormat(ctx->client,
                        "wrong number of arguments for '%s' command",
                        (char *)ctx->client->argv[0]->ptr);
    return REDISMODULE_OK;
}

/* Return the client object the `RM_Reply*` functions should target.
 * Normally this is just `ctx->client`, that is the client that called
 * the module command, however in the case of thread safe contexts there
 * is no directly associated client (since it would not be safe to access
 * the client from a thread), so instead the blocked client object referenced
 * in the thread safe context, has a fake client that we just use to accumulate
 * the replies. Later, when the client is unblocked, the accumulated replies
 * are appended to the actual client.
 *
 * The function returns the client pointer depending on the context, or
 * NULL if there is no potential client. This happens when we are in the
 * context of a thread safe context that was not initialized with a blocked
 * client object. Other contexts without associated clients are the ones
 * initialized to run the timers callbacks. */
client *moduleGetReplyClient(RedisModuleCtx *ctx)
{
    if (ctx->flags & REDISMODULE_CTX_THREAD_SAFE)
    {
        if (ctx->blocked_client)
            return ctx->blocked_client->reply_client;
        else
            return NULL;
    }
    else
    {
        /* If this is a non thread safe context, just return the client
         * that is running the command if any. This may be NULL as well
         * in the case of contexts that are not executed with associated
         * clients, like timer contexts. */
        return ctx->client;
    }
}

/* Send an integer reply to the client, with the specified long long value.
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithLongLong(RedisModuleCtx *ctx, long long ll)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    addReplyLongLong(c, ll);
    return REDISMODULE_OK;
}

/* Reply with an error or simple string (status message). Used to implement
 * ReplyWithSimpleString() and ReplyWithError().
 * The function always returns REDISMODULE_OK. */
int replyWithStatus(RedisModuleCtx *ctx, const char *msg, char *prefix)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    sds strmsg = sdsnewlen(prefix, 1);
    strmsg = sdscat(strmsg, msg);
    strmsg = sdscatlen(strmsg, "\r\n", 2);
    addReplySds(c, strmsg);
    return REDISMODULE_OK;
}

/* Reply with the error 'err'.
 *
 * Note that 'err' must contain all the error, including
 * the initial error code. The function only provides the initial "-", so
 * the usage is, for example:
 *
 *     RedisModule_ReplyWithError(ctx,"ERR Wrong Type");
 *
 * and not just:
 *
 *     RedisModule_ReplyWithError(ctx,"Wrong Type");
 *
 * The function always returns REDISMODULE_OK.
 */
int RM_ReplyWithError(RedisModuleCtx *ctx, const char *err)
{
    return replyWithStatus(ctx, err, "-");
}

/* Reply with a simple string (+... \r\n in RESP protocol). This replies
 * are suitable only when sending a small non-binary string with small
 * overhead, like "OK" or similar replies.
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithSimpleString(RedisModuleCtx *ctx, const char *msg)
{
    return replyWithStatus(ctx, msg, "+");
}

/* Reply with an array type of 'len' elements. However 'len' other calls
 * to `ReplyWith*` style functions must follow in order to emit the elements
 * of the array.
 *
 * When producing arrays with a number of element that is not known beforehand
 * the function can be called with the special count
 * REDISMODULE_POSTPONED_ARRAY_LEN, and the actual number of elements can be
 * later set with RedisModule_ReplySetArrayLength() (which will set the
 * latest "open" count if there are multiple ones).
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithArray(RedisModuleCtx *ctx, long len)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    if (len == REDISMODULE_POSTPONED_ARRAY_LEN)
    {
        ctx->postponed_arrays = zrealloc(ctx->postponed_arrays, sizeof(void *) *
                                                                    (ctx->postponed_arrays_count + 1));
        ctx->postponed_arrays[ctx->postponed_arrays_count] =
            addDeferredMultiBulkLength(c);
        ctx->postponed_arrays_count++;
    }
    else
    {
        addReplyMultiBulkLen(c, len);
    }
    return REDISMODULE_OK;
}

/* When RedisModule_ReplyWithArray() is used with the argument
 * REDISMODULE_POSTPONED_ARRAY_LEN, because we don't know beforehand the number
 * of items we are going to output as elements of the array, this function
 * will take care to set the array length.
 *
 * Since it is possible to have multiple array replies pending with unknown
 * length, this function guarantees to always set the latest array length
 * that was created in a postponed way.
 *
 * For example in order to output an array like [1,[10,20,30]] we
 * could write:
 *
 *      RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
 *      RedisModule_ReplyWithLongLong(ctx,1);
 *      RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
 *      RedisModule_ReplyWithLongLong(ctx,10);
 *      RedisModule_ReplyWithLongLong(ctx,20);
 *      RedisModule_ReplyWithLongLong(ctx,30);
 *      RedisModule_ReplySetArrayLength(ctx,3); // Set len of 10,20,30 array.
 *      RedisModule_ReplySetArrayLength(ctx,2); // Set len of top array
 *
 * Note that in the above example there is no reason to postpone the array
 * length, since we produce a fixed number of elements, but in the practice
 * the code may use an iterator or other ways of creating the output so
 * that is not easy to calculate in advance the number of elements.
 */
void RM_ReplySetArrayLength(RedisModuleCtx *ctx, long len)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return;
    if (ctx->postponed_arrays_count == 0)
    {
        serverLog(LL_WARNING,
                  "API misuse detected in module %s: "
                  "RedisModule_ReplySetArrayLength() called without previous "
                  "RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN) "
                  "call.",
                  ctx->module->name);
        return;
    }
    ctx->postponed_arrays_count--;
    setDeferredMultiBulkLength(c,
                               ctx->postponed_arrays[ctx->postponed_arrays_count],
                               len);
    if (ctx->postponed_arrays_count == 0)
    {
        zfree(ctx->postponed_arrays);
        ctx->postponed_arrays = NULL;
    }
}

/* Reply with a bulk string, taking in input a C buffer pointer and length.
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithStringBuffer(RedisModuleCtx *ctx, const char *buf, size_t len)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    addReplyBulkCBuffer(c, (char *)buf, len);
    return REDISMODULE_OK;
}

/* Reply with a bulk string, taking in input a C buffer pointer that is
 * assumed to be null-terminated.
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithCString(RedisModuleCtx *ctx, const char *buf)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    addReplyBulkCString(c, (char *)buf);
    return REDISMODULE_OK;
}

/* Reply with a bulk string, taking in input a RedisModuleString object.
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithString(RedisModuleCtx *ctx, RedisModuleString *str)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    addReplyBulk(c, str);
    return REDISMODULE_OK;
}

/* Reply to the client with a NULL. In the RESP protocol a NULL is encoded
 * as the string "$-1\r\n".
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithNull(RedisModuleCtx *ctx)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    addReply(c, shared.nullbulk);
    return REDISMODULE_OK;
}

/* Reply exactly what a Redis command returned us with RedisModule_Call().
 * This function is useful when we use RedisModule_Call() in order to
 * execute some command, as we want to reply to the client exactly the
 * same reply we obtained by the command.
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithCallReply(RedisModuleCtx *ctx, RedisModuleCallReply *reply)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    sds proto = sdsnewlen(reply->proto, reply->protolen);
    addReplySds(c, proto);
    return REDISMODULE_OK;
}

/* Send a string reply obtained converting the double 'd' into a bulk string.
 * This function is basically equivalent to converting a double into
 * a string into a C buffer, and then calling the function
 * RedisModule_ReplyWithStringBuffer() with the buffer and length.
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplyWithDouble(RedisModuleCtx *ctx, double d)
{
    client *c = moduleGetReplyClient(ctx);
    if (c == NULL)
        return REDISMODULE_OK;
    addReplyDouble(c, d);
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Commands replication API
 * -------------------------------------------------------------------------- */

/* Helper function to replicate MULTI the first time we replicate something
 * in the context of a command execution. EXEC will be handled by the
 * RedisModuleCommandDispatcher() function. */
void moduleReplicateMultiIfNeeded(RedisModuleCtx *ctx)
{
    /* Skip this if client explicitly wrap the command with MULTI, or if
     * the module command was called by a script. */
    if (ctx->client->flags & (CLIENT_MULTI | CLIENT_LUA))
        return;
    /* If we already emitted MULTI return ASAP. */
    if (ctx->flags & REDISMODULE_CTX_MULTI_EMITTED)
        return;
    /* If this is a thread safe context, we do not want to wrap commands
     * executed into MULTI/EXEC, they are executed as single commands
     * from an external client in essence. */
    if (ctx->flags & REDISMODULE_CTX_THREAD_SAFE)
        return;
    /* If this is a callback context, and not a module command execution
     * context, we have to setup the op array for the "also propagate" API
     * so that RM_Replicate() will work. */
    if (!(ctx->flags & REDISMODULE_CTX_MODULE_COMMAND_CALL))
    {
        ctx->saved_oparray = server.also_propagate;
        redisOpArrayInit(&server.also_propagate);
    }
    execCommandPropagateMulti(ctx->client);
    ctx->flags |= REDISMODULE_CTX_MULTI_EMITTED;
}

/* Replicate the specified command and arguments to slaves and AOF, as effect
 * of execution of the calling command implementation.
 *
 * The replicated commands are always wrapped into the MULTI/EXEC that
 * contains all the commands replicated in a given module command
 * execution. However the commands replicated with RedisModule_Call()
 * are the first items, the ones replicated with RedisModule_Replicate()
 * will all follow before the EXEC.
 *
 * Modules should try to use one interface or the other.
 *
 * This command follows exactly the same interface of RedisModule_Call(),
 * so a set of format specifiers must be passed, followed by arguments
 * matching the provided format specifiers.
 *
 * Please refer to RedisModule_Call() for more information.
 *
 * Using the special "A" and "R" modifiers, the caller can exclude either
 * the AOF or the replicas from the propagation of the specified command.
 * Otherwise, by default, the command will be propagated in both channels.
 *
 * ## Note about calling this function from a thread safe context:
 *
 * Normally when you call this function from the callback implementing a
 * module command, or any other callback provided by the Redis Module API,
 * Redis will accumulate all the calls to this function in the context of
 * the callback, and will propagate all the commands wrapped in a MULTI/EXEC
 * transaction. However when calling this function from a threaded safe context
 * that can live an undefined amount of time, and can be locked/unlocked in
 * at will, the behavior is different: MULTI/EXEC wrapper is not emitted
 * and the command specified is inserted in the AOF and replication stream
 * immediately.
 *
 * ## Return value
 *
 * The command returns REDISMODULE_ERR if the format specifiers are invalid
 * or the command name does not belong to a known command. */
int RM_Replicate(RedisModuleCtx *ctx, const char *cmdname, const char *fmt, ...)
{
    struct redisCommand *cmd;
    robj **argv = NULL;
    int argc = 0, flags = 0, j;
    va_list ap;

    cmd = lookupCommandByCString((char *)cmdname);
    if (!cmd)
        return REDISMODULE_ERR;

    /* Create the client and dispatch the command. */
    va_start(ap, fmt);
    argv = moduleCreateArgvFromUserFormat(cmdname, fmt, &argc, &flags, ap);
    va_end(ap);
    if (argv == NULL)
        return REDISMODULE_ERR;

    /* Select the propagation target. Usually is AOF + replicas, however
     * the caller can exclude one or the other using the "A" or "R"
     * modifiers. */
    int target = 0;
    if (!(flags & REDISMODULE_ARGV_NO_AOF))
        target |= PROPAGATE_AOF;
    if (!(flags & REDISMODULE_ARGV_NO_REPLICAS))
        target |= PROPAGATE_REPL;

    /* Replicate! When we are in a threaded context, we want to just insert
     * the replicated command ASAP, since it is not clear when the context
     * will stop being used, so accumulating stuff does not make much sense,
     * nor we could easily use the alsoPropagate() API from threads. */
    if (ctx->flags & REDISMODULE_CTX_THREAD_SAFE)
    {
        propagate(cmd, ctx->client->db->id, argv, argc, target);
    }
    else
    {
        moduleReplicateMultiIfNeeded(ctx);
        alsoPropagate(cmd, ctx->client->db->id, argv, argc, target);
    }

    /* Release the argv. */
    for (j = 0; j < argc; j++)
        decrRefCount(argv[j]);
    zfree(argv);
    server.dirty++;
    return REDISMODULE_OK;
}

/* This function will replicate the command exactly as it was invoked
 * by the client. Note that this function will not wrap the command into
 * a MULTI/EXEC stanza, so it should not be mixed with other replication
 * commands.
 *
 * Basically this form of replication is useful when you want to propagate
 * the command to the slaves and AOF file exactly as it was called, since
 * the command can just be re-executed to deterministically re-create the
 * new state starting from the old one.
 *
 * The function always returns REDISMODULE_OK. */
int RM_ReplicateVerbatim(RedisModuleCtx *ctx)
{
    alsoPropagate(ctx->client->cmd, ctx->client->db->id,
                  ctx->client->argv, ctx->client->argc,
                  PROPAGATE_AOF | PROPAGATE_REPL);
    server.dirty++;
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * DB and Key APIs -- Generic API
 * -------------------------------------------------------------------------- */

/* Return the ID of the current client calling the currently active module
 * command. The returned ID has a few guarantees:
 *
 * 1. The ID is different for each different client, so if the same client
 *    executes a module command multiple times, it can be recognized as
 *    having the same ID, otherwise the ID will be different.
 * 2. The ID increases monotonically. Clients connecting to the server later
 *    are guaranteed to get IDs greater than any past ID previously seen.
 *
 * Valid IDs are from 1 to 2^64-1. If 0 is returned it means there is no way
 * to fetch the ID in the context the function was currently called. */
unsigned long long RM_GetClientId(RedisModuleCtx *ctx)
{
    if (ctx->client == NULL)
        return 0;
    return ctx->client->id;
}

/* Return the currently selected DB. */
int RM_GetSelectedDb(RedisModuleCtx *ctx)
{
    return ctx->client->db->id;
}

/* Return the current context's flags. The flags provide information on the
 * current request context (whether the client is a Lua script or in a MULTI),
 * and about the Redis instance in general, i.e replication and persistence.
 *
 * The available flags are:
 *
 *  * REDISMODULE_CTX_FLAGS_LUA: The command is running in a Lua script
 *
 *  * REDISMODULE_CTX_FLAGS_MULTI: The command is running inside a transaction
 *
 *  * REDISMODULE_CTX_FLAGS_REPLICATED: The command was sent over the replication
 *    link by the MASTER
 *
 *  * REDISMODULE_CTX_FLAGS_MASTER: The Redis instance is a master
 *
 *  * REDISMODULE_CTX_FLAGS_SLAVE: The Redis instance is a slave
 *
 *  * REDISMODULE_CTX_FLAGS_READONLY: The Redis instance is read-only
 *
 *  * REDISMODULE_CTX_FLAGS_CLUSTER: The Redis instance is in cluster mode
 *
 *  * REDISMODULE_CTX_FLAGS_AOF: The Redis instance has AOF enabled
 *
 *  * REDISMODULE_CTX_FLAGS_RDB: The instance has RDB enabled
 *
 *  * REDISMODULE_CTX_FLAGS_MAXMEMORY:  The instance has Maxmemory set
 *
 *  * REDISMODULE_CTX_FLAGS_EVICT:  Maxmemory is set and has an eviction
 *    policy that may delete keys
 *
 *  * REDISMODULE_CTX_FLAGS_OOM: Redis is out of memory according to the
 *    maxmemory setting.
 *
 *  * REDISMODULE_CTX_FLAGS_OOM_WARNING: Less than 25% of memory remains before
 *                                       reaching the maxmemory level.
 *
 *  * REDISMODULE_CTX_FLAGS_LOADING: Server is loading RDB/AOF
 *
 *  * REDISMODULE_CTX_FLAGS_REPLICA_IS_STALE: No active link with the master.
 *
 *  * REDISMODULE_CTX_FLAGS_REPLICA_IS_CONNECTING: The replica is trying to
 *                                                 connect with the master.
 *
 *  * REDISMODULE_CTX_FLAGS_REPLICA_IS_TRANSFERRING: Master -> Replica RDB
 *                                                   transfer is in progress.
 *
 *  * REDISMODULE_CTX_FLAGS_REPLICA_IS_ONLINE: The replica has an active link
 *                                             with its master. This is the
 *                                             contrary of STALE state.
 *
 *  * REDISMODULE_CTX_FLAGS_ACTIVE_CHILD: There is currently some background
 *                                        process active (RDB, AUX or module).
 */
int RM_GetContextFlags(RedisModuleCtx *ctx)
{

    int flags = 0;
    /* Client specific flags */
    if (ctx->client)
    {
        if (ctx->client->flags & CLIENT_LUA)
            flags |= REDISMODULE_CTX_FLAGS_LUA;
        if (ctx->client->flags & CLIENT_MULTI)
            flags |= REDISMODULE_CTX_FLAGS_MULTI;
        /* Module command recieved from MASTER, is replicated. */
        if (ctx->client->flags & CLIENT_MASTER)
            flags |= REDISMODULE_CTX_FLAGS_REPLICATED;
    }

    if (server.cluster_enabled)
        flags |= REDISMODULE_CTX_FLAGS_CLUSTER;

    if (server.loading)
        flags |= REDISMODULE_CTX_FLAGS_LOADING;

    /* Maxmemory and eviction policy */
    if (server.maxmemory > 0)
    {
        flags |= REDISMODULE_CTX_FLAGS_MAXMEMORY;

        if (server.maxmemory_policy != MAXMEMORY_NO_EVICTION)
            flags |= REDISMODULE_CTX_FLAGS_EVICT;
    }

    /* Persistence flags */
    if (server.aof_state != AOF_OFF)
        flags |= REDISMODULE_CTX_FLAGS_AOF;
    if (server.saveparamslen > 0)
        flags |= REDISMODULE_CTX_FLAGS_RDB;

    /* Replication flags */
    if (server.masterhost == NULL)
    {
        flags |= REDISMODULE_CTX_FLAGS_MASTER;
    }
    else
    {
        flags |= REDISMODULE_CTX_FLAGS_SLAVE;
        if (server.repl_slave_ro)
            flags |= REDISMODULE_CTX_FLAGS_READONLY;

        /* Replica state flags. */
        if (server.repl_state == REPL_STATE_CONNECT ||
            server.repl_state == REPL_STATE_CONNECTING)
        {
            flags |= REDISMODULE_CTX_FLAGS_REPLICA_IS_CONNECTING;
        }
        else if (server.repl_state == REPL_STATE_TRANSFER)
        {
            flags |= REDISMODULE_CTX_FLAGS_REPLICA_IS_TRANSFERRING;
        }
        else if (server.repl_state == REPL_STATE_CONNECTED)
        {
            flags |= REDISMODULE_CTX_FLAGS_REPLICA_IS_ONLINE;
        }

        if (server.repl_state != REPL_STATE_CONNECTED)
            flags |= REDISMODULE_CTX_FLAGS_REPLICA_IS_STALE;
    }

    /* OOM flag. */
    float level;
    int retval = getMaxmemoryState(NULL, NULL, NULL, &level);
    if (retval == C_ERR)
        flags |= REDISMODULE_CTX_FLAGS_OOM;
    if (level > 0.75)
        flags |= REDISMODULE_CTX_FLAGS_OOM_WARNING;

    /* Presence of children processes. */
    if (hasActiveChildProcess())
        flags |= REDISMODULE_CTX_FLAGS_ACTIVE_CHILD;

    return flags;
}

/* Change the currently selected DB. Returns an error if the id
 * is out of range.
 *
 * Note that the client will retain the currently selected DB even after
 * the Redis command implemented by the module calling this function
 * returns.
 *
 * If the module command wishes to change something in a different DB and
 * returns back to the original one, it should call RedisModule_GetSelectedDb()
 * before in order to restore the old DB number before returning. */
int RM_SelectDb(RedisModuleCtx *ctx, int newid)
{
    int retval = selectDb(ctx->client, newid);
    return (retval == C_OK) ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Return an handle representing a Redis key, so that it is possible
 * to call other APIs with the key handle as argument to perform
 * operations on the key.
 *
 * The return value is the handle representing the key, that must be
 * closed with RM_CloseKey().
 *
 * If the key does not exist and WRITE mode is requested, the handle
 * is still returned, since it is possible to perform operations on
 * a yet not existing key (that will be created, for example, after
 * a list push operation). If the mode is just READ instead, and the
 * key does not exist, NULL is returned. However it is still safe to
 * call RedisModule_CloseKey() and RedisModule_KeyType() on a NULL
 * value. */
void *RM_OpenKey(RedisModuleCtx *ctx, robj *keyname, int mode)
{
    RedisModuleKey *kp;
    robj *value;

    if (mode & REDISMODULE_WRITE)
    {
        value = lookupKeyWrite(ctx->client->db, keyname);
    }
    else
    {
        value = lookupKeyRead(ctx->client->db, keyname);
        if (value == NULL)
        {
            return NULL;
        }
    }

    /* Setup the key handle. */
    kp = zmalloc(sizeof(*kp));
    kp->ctx = ctx;
    kp->db = ctx->client->db;
    kp->key = keyname;
    incrRefCount(keyname);
    kp->value = value;
    kp->iter = NULL;
    kp->mode = mode;
    zsetKeyReset(kp);
    autoMemoryAdd(ctx, REDISMODULE_AM_KEY, kp);
    return (void *)kp;
}

/* Close a key handle. */
void RM_CloseKey(RedisModuleKey *key)
{
    if (key == NULL)
        return;
    if (key->mode & REDISMODULE_WRITE)
        signalModifiedKey(key->db, key->key);
    /* TODO: if (key->iter) RM_KeyIteratorStop(kp); */
    RM_ZsetRangeStop(key);
    decrRefCount(key->key);
    autoMemoryFreed(key->ctx, REDISMODULE_AM_KEY, key);
    zfree(key);
}

/* Return the type of the key. If the key pointer is NULL then
 * REDISMODULE_KEYTYPE_EMPTY is returned. */
int RM_KeyType(RedisModuleKey *key)
{
    if (key == NULL || key->value == NULL)
        return REDISMODULE_KEYTYPE_EMPTY;
    /* We map between defines so that we are free to change the internal
     * defines as desired. */
    switch (key->value->type)
    {
    case OBJ_STRING:
        return REDISMODULE_KEYTYPE_STRING;
    case OBJ_LIST:
        return REDISMODULE_KEYTYPE_LIST;
    case OBJ_SET:
        return REDISMODULE_KEYTYPE_SET;
    case OBJ_ZSET:
        return REDISMODULE_KEYTYPE_ZSET;
    case OBJ_HASH:
        return REDISMODULE_KEYTYPE_HASH;
    case OBJ_MODULE:
        return REDISMODULE_KEYTYPE_MODULE;
    default:
        return 0;
    }
}

/* Return the length of the value associated with the key.
 * For strings this is the length of the string. For all the other types
 * is the number of elements (just counting keys for hashes).
 *
 * If the key pointer is NULL or the key is empty, zero is returned. */
size_t RM_ValueLength(RedisModuleKey *key)
{
    if (key == NULL || key->value == NULL)
        return 0;
    switch (key->value->type)
    {
    case OBJ_STRING:
        return stringObjectLen(key->value);
    case OBJ_LIST:
        return listTypeLength(key->value);
    case OBJ_SET:
        return setTypeSize(key->value);
    case OBJ_ZSET:
        return zsetLength(key->value);
    case OBJ_HASH:
        return hashTypeLength(key->value);
    default:
        return 0;
    }
}

/* If the key is open for writing, remove it, and setup the key to
 * accept new writes as an empty key (that will be created on demand).
 * On success REDISMODULE_OK is returned. If the key is not open for
 * writing REDISMODULE_ERR is returned. */
int RM_DeleteKey(RedisModuleKey *key)
{
    if (!(key->mode & REDISMODULE_WRITE))
        return REDISMODULE_ERR;
    if (key->value)
    {
        dbDelete(key->db, key->key);
        key->value = NULL;
    }
    return REDISMODULE_OK;
}

/* If the key is open for writing, unlink it (that is delete it in a
 * non-blocking way, not reclaiming memory immediately) and setup the key to
 * accept new writes as an empty key (that will be created on demand).
 * On success REDISMODULE_OK is returned. If the key is not open for
 * writing REDISMODULE_ERR is returned. */
int RM_UnlinkKey(RedisModuleKey *key)
{
    if (!(key->mode & REDISMODULE_WRITE))
        return REDISMODULE_ERR;
    if (key->value)
    {
        dbAsyncDelete(key->db, key->key);
        key->value = NULL;
    }
    return REDISMODULE_OK;
}

/* Return the key expire value, as milliseconds of remaining TTL.
 * If no TTL is associated with the key or if the key is empty,
 * REDISMODULE_NO_EXPIRE is returned. */
mstime_t RM_GetExpire(RedisModuleKey *key)
{
    mstime_t expire = getExpire(key->db, key->key);
    if (expire == -1 || key->value == NULL)
        return -1;
    expire -= mstime();
    return expire >= 0 ? expire : 0;
}

/* Set a new expire for the key. If the special expire
 * REDISMODULE_NO_EXPIRE is set, the expire is cancelled if there was
 * one (the same as the PERSIST command).
 *
 * Note that the expire must be provided as a positive integer representing
 * the number of milliseconds of TTL the key should have.
 *
 * The function returns REDISMODULE_OK on success or REDISMODULE_ERR if
 * the key was not open for writing or is an empty key. */
int RM_SetExpire(RedisModuleKey *key, mstime_t expire)
{
    if (!(key->mode & REDISMODULE_WRITE) || key->value == NULL)
        return REDISMODULE_ERR;
    if (expire != REDISMODULE_NO_EXPIRE)
    {
        expire += mstime();
        setExpire(key->ctx->client, key->db, key->key, expire);
    }
    else
    {
        removeExpire(key->db, key->key);
    }
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Key API for String type
 * -------------------------------------------------------------------------- */

/* If the key is open for writing, set the specified string 'str' as the
 * value of the key, deleting the old value if any.
 * On success REDISMODULE_OK is returned. If the key is not open for
 * writing or there is an active iterator, REDISMODULE_ERR is returned. */
int RM_StringSet(RedisModuleKey *key, RedisModuleString *str)
{
    if (!(key->mode & REDISMODULE_WRITE) || key->iter)
        return REDISMODULE_ERR;
    RM_DeleteKey(key);
    setKey(key->db, key->key, str);
    key->value = str;
    return REDISMODULE_OK;
}

/* Prepare the key associated string value for DMA access, and returns
 * a pointer and size (by reference), that the user can use to read or
 * modify the string in-place accessing it directly via pointer.
 *
 * The 'mode' is composed by bitwise OR-ing the following flags:
 *
 *     REDISMODULE_READ -- Read access
 *     REDISMODULE_WRITE -- Write access
 *
 * If the DMA is not requested for writing, the pointer returned should
 * only be accessed in a read-only fashion.
 *
 * On error (wrong type) NULL is returned.
 *
 * DMA access rules:
 *
 * 1. No other key writing function should be called since the moment
 * the pointer is obtained, for all the time we want to use DMA access
 * to read or modify the string.
 *
 * 2. Each time RM_StringTruncate() is called, to continue with the DMA
 * access, RM_StringDMA() should be called again to re-obtain
 * a new pointer and length.
 *
 * 3. If the returned pointer is not NULL, but the length is zero, no
 * byte can be touched (the string is empty, or the key itself is empty)
 * so a RM_StringTruncate() call should be used if there is to enlarge
 * the string, and later call StringDMA() again to get the pointer.
 */
char *RM_StringDMA(RedisModuleKey *key, size_t *len, int mode)
{
    /* We need to return *some* pointer for empty keys, we just return
     * a string literal pointer, that is the advantage to be mapped into
     * a read only memory page, so the module will segfault if a write
     * attempt is performed. */
    char *emptystring = "<dma-empty-string>";
    if (key->value == NULL)
    {
        *len = 0;
        return emptystring;
    }

    if (key->value->type != OBJ_STRING)
        return NULL;

    /* For write access, and even for read access if the object is encoded,
     * we unshare the string (that has the side effect of decoding it). */
    if ((mode & REDISMODULE_WRITE) || key->value->encoding != OBJ_ENCODING_RAW)
        key->value = dbUnshareStringValue(key->db, key->key, key->value);

    *len = sdslen(key->value->ptr);
    return key->value->ptr;
}

/* If the string is open for writing and is of string type, resize it, padding
 * with zero bytes if the new length is greater than the old one.
 *
 * After this call, RM_StringDMA() must be called again to continue
 * DMA access with the new pointer.
 *
 * The function returns REDISMODULE_OK on success, and REDISMODULE_ERR on
 * error, that is, the key is not open for writing, is not a string
 * or resizing for more than 512 MB is requested.
 *
 * If the key is empty, a string key is created with the new string value
 * unless the new length value requested is zero. */
int RM_StringTruncate(RedisModuleKey *key, size_t newlen)
{
    if (!(key->mode & REDISMODULE_WRITE))
        return REDISMODULE_ERR;
    if (key->value && key->value->type != OBJ_STRING)
        return REDISMODULE_ERR;
    if (newlen > 512 * 1024 * 1024)
        return REDISMODULE_ERR;

    /* Empty key and new len set to 0. Just return REDISMODULE_OK without
     * doing anything. */
    if (key->value == NULL && newlen == 0)
        return REDISMODULE_OK;

    if (key->value == NULL)
    {
        /* Empty key: create it with the new size. */
        robj *o = createObject(OBJ_STRING, sdsnewlen(NULL, newlen));
        setKey(key->db, key->key, o);
        key->value = o;
        decrRefCount(o);
    }
    else
    {
        /* Unshare and resize. */
        key->value = dbUnshareStringValue(key->db, key->key, key->value);
        size_t curlen = sdslen(key->value->ptr);
        if (newlen > curlen)
        {
            key->value->ptr = sdsgrowzero(key->value->ptr, newlen);
        }
        else if (newlen < curlen)
        {
            sdsrange(key->value->ptr, 0, newlen - 1);
            /* If the string is too wasteful, reallocate it. */
            if (sdslen(key->value->ptr) < sdsavail(key->value->ptr))
                key->value->ptr = sdsRemoveFreeSpace(key->value->ptr);
        }
    }
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Key API for List type
 * -------------------------------------------------------------------------- */

/* Push an element into a list, on head or tail depending on 'where' argument.
 * If the key pointer is about an empty key opened for writing, the key
 * is created. On error (key opened for read-only operations or of the wrong
 * type) REDISMODULE_ERR is returned, otherwise REDISMODULE_OK is returned. */
int RM_ListPush(RedisModuleKey *key, int where, RedisModuleString *ele)
{
    if (!(key->mode & REDISMODULE_WRITE))
        return REDISMODULE_ERR;
    if (key->value && key->value->type != OBJ_LIST)
        return REDISMODULE_ERR;
    if (key->value == NULL)
        moduleCreateEmptyKey(key, REDISMODULE_KEYTYPE_LIST);
    listTypePush(key->value, ele,
                 (where == REDISMODULE_LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL);
    return REDISMODULE_OK;
}

/* Pop an element from the list, and returns it as a module string object
 * that the user should be free with RM_FreeString() or by enabling
 * automatic memory. 'where' specifies if the element should be popped from
 * head or tail. The command returns NULL if:
 * 1) The list is empty.
 * 2) The key was not open for writing.
 * 3) The key is not a list. */
RedisModuleString *RM_ListPop(RedisModuleKey *key, int where)
{
    if (!(key->mode & REDISMODULE_WRITE) ||
        key->value == NULL ||
        key->value->type != OBJ_LIST)
        return NULL;
    robj *ele = listTypePop(key->value,
                            (where == REDISMODULE_LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL);
    robj *decoded = getDecodedObject(ele);
    decrRefCount(ele);
    moduleDelKeyIfEmpty(key);
    autoMemoryAdd(key->ctx, REDISMODULE_AM_STRING, decoded);
    return decoded;
}

/* --------------------------------------------------------------------------
 * Key API for Sorted Set type
 * -------------------------------------------------------------------------- */

/* Conversion from/to public flags of the Modules API and our private flags,
 * so that we have everything decoupled. */
int RM_ZsetAddFlagsToCoreFlags(int flags)
{
    int retflags = 0;
    if (flags & REDISMODULE_ZADD_XX)
        retflags |= ZADD_XX;
    if (flags & REDISMODULE_ZADD_NX)
        retflags |= ZADD_NX;
    return retflags;
}

/* See previous function comment. */
int RM_ZsetAddFlagsFromCoreFlags(int flags)
{
    int retflags = 0;
    if (flags & ZADD_ADDED)
        retflags |= REDISMODULE_ZADD_ADDED;
    if (flags & ZADD_UPDATED)
        retflags |= REDISMODULE_ZADD_UPDATED;
    if (flags & ZADD_NOP)
        retflags |= REDISMODULE_ZADD_NOP;
    return retflags;
}

/* Add a new element into a sorted set, with the specified 'score'.
 * If the element already exists, the score is updated.
 *
 * A new sorted set is created at value if the key is an empty open key
 * setup for writing.
 *
 * Additional flags can be passed to the function via a pointer, the flags
 * are both used to receive input and to communicate state when the function
 * returns. 'flagsptr' can be NULL if no special flags are used.
 *
 * The input flags are:
 *
 *     REDISMODULE_ZADD_XX: Element must already exist. Do nothing otherwise.
 *     REDISMODULE_ZADD_NX: Element must not exist. Do nothing otherwise.
 *
 * The output flags are:
 *
 *     REDISMODULE_ZADD_ADDED: The new element was added to the sorted set.
 *     REDISMODULE_ZADD_UPDATED: The score of the element was updated.
 *     REDISMODULE_ZADD_NOP: No operation was performed because XX or NX flags.
 *
 * On success the function returns REDISMODULE_OK. On the following errors
 * REDISMODULE_ERR is returned:
 *
 * * The key was not opened for writing.
 * * The key is of the wrong type.
 * * 'score' double value is not a number (NaN).
 */
int RM_ZsetAdd(RedisModuleKey *key, double score, RedisModuleString *ele, int *flagsptr)
{
    int flags = 0;
    if (!(key->mode & REDISMODULE_WRITE))
        return REDISMODULE_ERR;
    if (key->value && key->value->type != OBJ_ZSET)
        return REDISMODULE_ERR;
    if (key->value == NULL)
        moduleCreateEmptyKey(key, REDISMODULE_KEYTYPE_ZSET);
    if (flagsptr)
        flags = RM_ZsetAddFlagsToCoreFlags(*flagsptr);
    if (zsetAdd(key->value, score, ele->ptr, &flags, NULL) == 0)
    {
        if (flagsptr)
            *flagsptr = 0;
        return REDISMODULE_ERR;
    }
    if (flagsptr)
        *flagsptr = RM_ZsetAddFlagsFromCoreFlags(flags);
    return REDISMODULE_OK;
}

/* This function works exactly like RM_ZsetAdd(), but instead of setting
 * a new score, the score of the existing element is incremented, or if the
 * element does not already exist, it is added assuming the old score was
 * zero.
 *
 * The input and output flags, and the return value, have the same exact
 * meaning, with the only difference that this function will return
 * REDISMODULE_ERR even when 'score' is a valid double number, but adding it
 * to the existing score results into a NaN (not a number) condition.
 *
 * This function has an additional field 'newscore', if not NULL is filled
 * with the new score of the element after the increment, if no error
 * is returned. */
int RM_ZsetIncrby(RedisModuleKey *key, double score, RedisModuleString *ele, int *flagsptr, double *newscore)
{
    int flags = 0;
    if (!(key->mode & REDISMODULE_WRITE))
        return REDISMODULE_ERR;
    if (key->value && key->value->type != OBJ_ZSET)
        return REDISMODULE_ERR;
    if (key->value == NULL)
        moduleCreateEmptyKey(key, REDISMODULE_KEYTYPE_ZSET);
    if (flagsptr)
        flags = RM_ZsetAddFlagsToCoreFlags(*flagsptr);
    flags |= ZADD_INCR;
    if (zsetAdd(key->value, score, ele->ptr, &flags, newscore) == 0)
    {
        if (flagsptr)
            *flagsptr = 0;
        return REDISMODULE_ERR;
    }
    /* zsetAdd() may signal back that the resulting score is not a number. */
    if (flagsptr && (*flagsptr & ZADD_NAN))
    {
        *flagsptr = 0;
        return REDISMODULE_ERR;
    }
    if (flagsptr)
        *flagsptr = RM_ZsetAddFlagsFromCoreFlags(flags);
    return REDISMODULE_OK;
}

/* Remove the specified element from the sorted set.
 * The function returns REDISMODULE_OK on success, and REDISMODULE_ERR
 * on one of the following conditions:
 *
 * * The key was not opened for writing.
 * * The key is of the wrong type.
 *
 * The return value does NOT indicate the fact the element was really
 * removed (since it existed) or not, just if the function was executed
 * with success.
 *
 * In order to know if the element was removed, the additional argument
 * 'deleted' must be passed, that populates the integer by reference
 * setting it to 1 or 0 depending on the outcome of the operation.
 * The 'deleted' argument can be NULL if the caller is not interested
 * to know if the element was really removed.
 *
 * Empty keys will be handled correctly by doing nothing. */
int RM_ZsetRem(RedisModuleKey *key, RedisModuleString *ele, int *deleted)
{
    if (!(key->mode & REDISMODULE_WRITE))
        return REDISMODULE_ERR;
    if (key->value && key->value->type != OBJ_ZSET)
        return REDISMODULE_ERR;
    if (key->value != NULL && zsetDel(key->value, ele->ptr))
    {
        if (deleted)
            *deleted = 1;
    }
    else
    {
        if (deleted)
            *deleted = 0;
    }
    return REDISMODULE_OK;
}

/* On success retrieve the double score associated at the sorted set element
 * 'ele' and returns REDISMODULE_OK. Otherwise REDISMODULE_ERR is returned
 * to signal one of the following conditions:
 *
 * * There is no such element 'ele' in the sorted set.
 * * The key is not a sorted set.
 * * The key is an open empty key.
 */
int RM_ZsetScore(RedisModuleKey *key, RedisModuleString *ele, double *score)
{
    if (key->value == NULL)
        return REDISMODULE_ERR;
    if (key->value->type != OBJ_ZSET)
        return REDISMODULE_ERR;
    if (zsetScore(key->value, ele->ptr, score) == C_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Key API for Sorted Set iterator
 * -------------------------------------------------------------------------- */

void zsetKeyReset(RedisModuleKey *key)
{
    key->ztype = REDISMODULE_ZSET_RANGE_NONE;
    key->zcurrent = NULL;
    key->zer = 1;
}

/* Stop a sorted set iteration. */
void RM_ZsetRangeStop(RedisModuleKey *key)
{
    /* Free resources if needed. */
    if (key->ztype == REDISMODULE_ZSET_RANGE_LEX)
        zslFreeLexRange(&key->zlrs);
    /* Setup sensible values so that misused iteration API calls when an
     * iterator is not active will result into something more sensible
     * than crashing. */
    zsetKeyReset(key);
}

/* Return the "End of range" flag value to signal the end of the iteration. */
int RM_ZsetRangeEndReached(RedisModuleKey *key)
{
    return key->zer;
}

/* Helper function for RM_ZsetFirstInScoreRange() and RM_ZsetLastInScoreRange().
 * Setup the sorted set iteration according to the specified score range
 * (see the functions calling it for more info). If 'first' is true the
 * first element in the range is used as a starting point for the iterator
 * otherwise the last. Return REDISMODULE_OK on success otherwise
 * REDISMODULE_ERR. */
int zsetInitScoreRange(RedisModuleKey *key, double min, double max, int minex, int maxex, int first)
{
    if (!key->value || key->value->type != OBJ_ZSET)
        return REDISMODULE_ERR;

    RM_ZsetRangeStop(key);
    key->ztype = REDISMODULE_ZSET_RANGE_SCORE;
    key->zer = 0;

    /* Setup the range structure used by the sorted set core implementation
     * in order to seek at the specified element. */
    zrangespec *zrs = &key->zrs;
    zrs->min = min;
    zrs->max = max;
    zrs->minex = minex;
    zrs->maxex = maxex;

    if (key->value->encoding == OBJ_ENCODING_ZIPLIST)
    {
        key->zcurrent = first ? zzlFirstInRange(key->value->ptr, zrs) : zzlLastInRange(key->value->ptr, zrs);
    }
    else if (key->value->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zset *zs = key->value->ptr;
        zskiplist *zsl = zs->zsl;
        key->zcurrent = first ? zslFirstInRange(zsl, zrs) : zslLastInRange(zsl, zrs);
    }
    else
    {
        serverPanic("Unsupported zset encoding");
    }
    if (key->zcurrent == NULL)
        key->zer = 1;
    return REDISMODULE_OK;
}

/* Setup a sorted set iterator seeking the first element in the specified
 * range. Returns REDISMODULE_OK if the iterator was correctly initialized
 * otherwise REDISMODULE_ERR is returned in the following conditions:
 *
 * 1. The value stored at key is not a sorted set or the key is empty.
 *
 * The range is specified according to the two double values 'min' and 'max'.
 * Both can be infinite using the following two macros:
 *
 * REDISMODULE_POSITIVE_INFINITE for positive infinite value
 * REDISMODULE_NEGATIVE_INFINITE for negative infinite value
 *
 * 'minex' and 'maxex' parameters, if true, respectively setup a range
 * where the min and max value are exclusive (not included) instead of
 * inclusive. */
int RM_ZsetFirstInScoreRange(RedisModuleKey *key, double min, double max, int minex, int maxex)
{
    return zsetInitScoreRange(key, min, max, minex, maxex, 1);
}

/* Exactly like RedisModule_ZsetFirstInScoreRange() but the last element of
 * the range is selected for the start of the iteration instead. */
int RM_ZsetLastInScoreRange(RedisModuleKey *key, double min, double max, int minex, int maxex)
{
    return zsetInitScoreRange(key, min, max, minex, maxex, 0);
}

/* Helper function for RM_ZsetFirstInLexRange() and RM_ZsetLastInLexRange().
 * Setup the sorted set iteration according to the specified lexicographical
 * range (see the functions calling it for more info). If 'first' is true the
 * first element in the range is used as a starting point for the iterator
 * otherwise the last. Return REDISMODULE_OK on success otherwise
 * REDISMODULE_ERR.
 *
 * Note that this function takes 'min' and 'max' in the same form of the
 * Redis ZRANGEBYLEX command. */
int zsetInitLexRange(RedisModuleKey *key, RedisModuleString *min, RedisModuleString *max, int first)
{
    if (!key->value || key->value->type != OBJ_ZSET)
        return REDISMODULE_ERR;

    RM_ZsetRangeStop(key);
    key->zer = 0;

    /* Setup the range structure used by the sorted set core implementation
     * in order to seek at the specified element. */
    zlexrangespec *zlrs = &key->zlrs;
    if (zslParseLexRange(min, max, zlrs) == C_ERR)
        return REDISMODULE_ERR;

    /* Set the range type to lex only after successfully parsing the range,
     * otherwise we don't want the zlexrangespec to be freed. */
    key->ztype = REDISMODULE_ZSET_RANGE_LEX;

    if (key->value->encoding == OBJ_ENCODING_ZIPLIST)
    {
        key->zcurrent = first ? zzlFirstInLexRange(key->value->ptr, zlrs) : zzlLastInLexRange(key->value->ptr, zlrs);
    }
    else if (key->value->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zset *zs = key->value->ptr;
        zskiplist *zsl = zs->zsl;
        key->zcurrent = first ? zslFirstInLexRange(zsl, zlrs) : zslLastInLexRange(zsl, zlrs);
    }
    else
    {
        serverPanic("Unsupported zset encoding");
    }
    if (key->zcurrent == NULL)
        key->zer = 1;

    return REDISMODULE_OK;
}

/* Setup a sorted set iterator seeking the first element in the specified
 * lexicographical range. Returns REDISMODULE_OK if the iterator was correctly
 * initialized otherwise REDISMODULE_ERR is returned in the
 * following conditions:
 *
 * 1. The value stored at key is not a sorted set or the key is empty.
 * 2. The lexicographical range 'min' and 'max' format is invalid.
 *
 * 'min' and 'max' should be provided as two RedisModuleString objects
 * in the same format as the parameters passed to the ZRANGEBYLEX command.
 * The function does not take ownership of the objects, so they can be released
 * ASAP after the iterator is setup. */
int RM_ZsetFirstInLexRange(RedisModuleKey *key, RedisModuleString *min, RedisModuleString *max)
{
    return zsetInitLexRange(key, min, max, 1);
}

/* Exactly like RedisModule_ZsetFirstInLexRange() but the last element of
 * the range is selected for the start of the iteration instead. */
int RM_ZsetLastInLexRange(RedisModuleKey *key, RedisModuleString *min, RedisModuleString *max)
{
    return zsetInitLexRange(key, min, max, 0);
}

/* Return the current sorted set element of an active sorted set iterator
 * or NULL if the range specified in the iterator does not include any
 * element. */
RedisModuleString *RM_ZsetRangeCurrentElement(RedisModuleKey *key, double *score)
{
    RedisModuleString *str;

    if (key->zcurrent == NULL)
        return NULL;
    if (key->value->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *eptr, *sptr;
        eptr = key->zcurrent;
        sds ele = ziplistGetObject(eptr);
        if (score)
        {
            sptr = ziplistNext(key->value->ptr, eptr);
            *score = zzlGetScore(sptr);
        }
        str = createObject(OBJ_STRING, ele);
    }
    else if (key->value->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zskiplistNode *ln = key->zcurrent;
        if (score)
            *score = ln->score;
        str = createStringObject(ln->ele, sdslen(ln->ele));
    }
    else
    {
        serverPanic("Unsupported zset encoding");
    }
    autoMemoryAdd(key->ctx, REDISMODULE_AM_STRING, str);
    return str;
}

/* Go to the next element of the sorted set iterator. Returns 1 if there was
 * a next element, 0 if we are already at the latest element or the range
 * does not include any item at all. */
int RM_ZsetRangeNext(RedisModuleKey *key)
{
    if (!key->ztype || !key->zcurrent)
        return 0; /* No active iterator. */

    if (key->value->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *zl = key->value->ptr;
        unsigned char *eptr = key->zcurrent;
        unsigned char *next;
        next = ziplistNext(zl, eptr); /* Skip element. */
        if (next)
            next = ziplistNext(zl, next); /* Skip score. */
        if (next == NULL)
        {
            key->zer = 1;
            return 0;
        }
        else
        {
            /* Are we still within the range? */
            if (key->ztype == REDISMODULE_ZSET_RANGE_SCORE)
            {
                /* Fetch the next element score for the
                 * range check. */
                unsigned char *saved_next = next;
                next = ziplistNext(zl, next);     /* Skip next element. */
                double score = zzlGetScore(next); /* Obtain the next score. */
                if (!zslValueLteMax(score, &key->zrs))
                {
                    key->zer = 1;
                    return 0;
                }
                next = saved_next;
            }
            else if (key->ztype == REDISMODULE_ZSET_RANGE_LEX)
            {
                if (!zzlLexValueLteMax(next, &key->zlrs))
                {
                    key->zer = 1;
                    return 0;
                }
            }
            key->zcurrent = next;
            return 1;
        }
    }
    else if (key->value->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zskiplistNode *ln = key->zcurrent, *next = ln->level[0].forward;
        if (next == NULL)
        {
            key->zer = 1;
            return 0;
        }
        else
        {
            /* Are we still within the range? */
            if (key->ztype == REDISMODULE_ZSET_RANGE_SCORE &&
                !zslValueLteMax(next->score, &key->zrs))
            {
                key->zer = 1;
                return 0;
            }
            else if (key->ztype == REDISMODULE_ZSET_RANGE_LEX)
            {
                if (!zslLexValueLteMax(next->ele, &key->zlrs))
                {
                    key->zer = 1;
                    return 0;
                }
            }
            key->zcurrent = next;
            return 1;
        }
    }
    else
    {
        serverPanic("Unsupported zset encoding");
    }
}

/* Go to the previous element of the sorted set iterator. Returns 1 if there was
 * a previous element, 0 if we are already at the first element or the range
 * does not include any item at all. */
int RM_ZsetRangePrev(RedisModuleKey *key)
{
    if (!key->ztype || !key->zcurrent)
        return 0; /* No active iterator. */

    if (key->value->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *zl = key->value->ptr;
        unsigned char *eptr = key->zcurrent;
        unsigned char *prev;
        prev = ziplistPrev(zl, eptr); /* Go back to previous score. */
        if (prev)
            prev = ziplistPrev(zl, prev); /* Back to previous ele. */
        if (prev == NULL)
        {
            key->zer = 1;
            return 0;
        }
        else
        {
            /* Are we still within the range? */
            if (key->ztype == REDISMODULE_ZSET_RANGE_SCORE)
            {
                /* Fetch the previous element score for the
                 * range check. */
                unsigned char *saved_prev = prev;
                prev = ziplistNext(zl, prev);     /* Skip element to get the score.*/
                double score = zzlGetScore(prev); /* Obtain the prev score. */
                if (!zslValueGteMin(score, &key->zrs))
                {
                    key->zer = 1;
                    return 0;
                }
                prev = saved_prev;
            }
            else if (key->ztype == REDISMODULE_ZSET_RANGE_LEX)
            {
                if (!zzlLexValueGteMin(prev, &key->zlrs))
                {
                    key->zer = 1;
                    return 0;
                }
            }
            key->zcurrent = prev;
            return 1;
        }
    }
    else if (key->value->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zskiplistNode *ln = key->zcurrent, *prev = ln->backward;
        if (prev == NULL)
        {
            key->zer = 1;
            return 0;
        }
        else
        {
            /* Are we still within the range? */
            if (key->ztype == REDISMODULE_ZSET_RANGE_SCORE &&
                !zslValueGteMin(prev->score, &key->zrs))
            {
                key->zer = 1;
                return 0;
            }
            else if (key->ztype == REDISMODULE_ZSET_RANGE_LEX)
            {
                if (!zslLexValueGteMin(prev->ele, &key->zlrs))
                {
                    key->zer = 1;
                    return 0;
                }
            }
            key->zcurrent = prev;
            return 1;
        }
    }
    else
    {
        serverPanic("Unsupported zset encoding");
    }
}

/* --------------------------------------------------------------------------
 * Key API for Hash type
 * -------------------------------------------------------------------------- */

/* Set the field of the specified hash field to the specified value.
 * If the key is an empty key open for writing, it is created with an empty
 * hash value, in order to set the specified field.
 *
 * The function is variadic and the user must specify pairs of field
 * names and values, both as RedisModuleString pointers (unless the
 * CFIELD option is set, see later). At the end of the field/value-ptr pairs,
 * NULL must be specified as last argument to signal the end of the arguments
 * in the variadic function.
 *
 * Example to set the hash argv[1] to the value argv[2]:
 *
 *      RedisModule_HashSet(key,REDISMODULE_HASH_NONE,argv[1],argv[2],NULL);
 *
 * The function can also be used in order to delete fields (if they exist)
 * by setting them to the specified value of REDISMODULE_HASH_DELETE:
 *
 *      RedisModule_HashSet(key,REDISMODULE_HASH_NONE,argv[1],
 *                          REDISMODULE_HASH_DELETE,NULL);
 *
 * The behavior of the command changes with the specified flags, that can be
 * set to REDISMODULE_HASH_NONE if no special behavior is needed.
 *
 *     REDISMODULE_HASH_NX: The operation is performed only if the field was not
 *                          already existing in the hash.
 *     REDISMODULE_HASH_XX: The operation is performed only if the field was
 *                          already existing, so that a new value could be
 *                          associated to an existing filed, but no new fields
 *                          are created.
 *     REDISMODULE_HASH_CFIELDS: The field names passed are null terminated C
 *                               strings instead of RedisModuleString objects.
 *
 * Unless NX is specified, the command overwrites the old field value with
 * the new one.
 *
 * When using REDISMODULE_HASH_CFIELDS, field names are reported using
 * normal C strings, so for example to delete the field "foo" the following
 * code can be used:
 *
 *      RedisModule_HashSet(key,REDISMODULE_HASH_CFIELDS,"foo",
 *                          REDISMODULE_HASH_DELETE,NULL);
 *
 * Return value:
 *
 * The number of fields updated (that may be less than the number of fields
 * specified because of the XX or NX options).
 *
 * In the following case the return value is always zero:
 *
 * * The key was not open for writing.
 * * The key was associated with a non Hash value.
 */
int RM_HashSet(RedisModuleKey *key, int flags, ...)
{
    va_list ap;
    if (!(key->mode & REDISMODULE_WRITE))
        return 0;
    if (key->value && key->value->type != OBJ_HASH)
        return 0;
    if (key->value == NULL)
        moduleCreateEmptyKey(key, REDISMODULE_KEYTYPE_HASH);

    int updated = 0;
    va_start(ap, flags);
    while (1)
    {
        RedisModuleString *field, *value;
        /* Get the field and value objects. */
        if (flags & REDISMODULE_HASH_CFIELDS)
        {
            char *cfield = va_arg(ap, char *);
            if (cfield == NULL)
                break;
            field = createRawStringObject(cfield, strlen(cfield));
        }
        else
        {
            field = va_arg(ap, RedisModuleString *);
            if (field == NULL)
                break;
        }
        value = va_arg(ap, RedisModuleString *);

        /* Handle XX and NX */
        if (flags & (REDISMODULE_HASH_XX | REDISMODULE_HASH_NX))
        {
            int exists = hashTypeExists(key->value, field->ptr);
            if (((flags & REDISMODULE_HASH_XX) && !exists) ||
                ((flags & REDISMODULE_HASH_NX) && exists))
            {
                if (flags & REDISMODULE_HASH_CFIELDS)
                    decrRefCount(field);
                continue;
            }
        }

        /* Handle deletion if value is REDISMODULE_HASH_DELETE. */
        if (value == REDISMODULE_HASH_DELETE)
        {
            updated += hashTypeDelete(key->value, field->ptr);
            if (flags & REDISMODULE_HASH_CFIELDS)
                decrRefCount(field);
            continue;
        }

        int low_flags = HASH_SET_COPY;
        /* If CFIELDS is active, we can pass the ownership of the
         * SDS object to the low level function that sets the field
         * to avoid a useless copy. */
        if (flags & REDISMODULE_HASH_CFIELDS)
            low_flags |= HASH_SET_TAKE_FIELD;

        robj *argv[2] = {field, value};
        hashTypeTryConversion(key->value, argv, 0, 1);
        updated += hashTypeSet(key->value, field->ptr, value->ptr, low_flags);

        /* If CFIELDS is active, SDS string ownership is now of hashTypeSet(),
         * however we still have to release the 'field' object shell. */
        if (flags & REDISMODULE_HASH_CFIELDS)
        {
            field->ptr = NULL; /* Prevent the SDS string from being freed. */
            decrRefCount(field);
        }
    }
    va_end(ap);
    moduleDelKeyIfEmpty(key);
    return updated;
}

/* Get fields from an hash value. This function is called using a variable
 * number of arguments, alternating a field name (as a StringRedisModule
 * pointer) with a pointer to a StringRedisModule pointer, that is set to the
 * value of the field if the field exist, or NULL if the field did not exist.
 * At the end of the field/value-ptr pairs, NULL must be specified as last
 * argument to signal the end of the arguments in the variadic function.
 *
 * This is an example usage:
 *
 *      RedisModuleString *first, *second;
 *      RedisModule_HashGet(mykey,REDISMODULE_HASH_NONE,argv[1],&first,
 *                      argv[2],&second,NULL);
 *
 * As with RedisModule_HashSet() the behavior of the command can be specified
 * passing flags different than REDISMODULE_HASH_NONE:
 *
 * REDISMODULE_HASH_CFIELD: field names as null terminated C strings.
 *
 * REDISMODULE_HASH_EXISTS: instead of setting the value of the field
 * expecting a RedisModuleString pointer to pointer, the function just
 * reports if the field esists or not and expects an integer pointer
 * as the second element of each pair.
 *
 * Example of REDISMODULE_HASH_CFIELD:
 *
 *      RedisModuleString *username, *hashedpass;
 *      RedisModule_HashGet(mykey,"username",&username,"hp",&hashedpass, NULL);
 *
 * Example of REDISMODULE_HASH_EXISTS:
 *
 *      int exists;
 *      RedisModule_HashGet(mykey,argv[1],&exists,NULL);
 *
 * The function returns REDISMODULE_OK on success and REDISMODULE_ERR if
 * the key is not an hash value.
 *
 * Memory management:
 *
 * The returned RedisModuleString objects should be released with
 * RedisModule_FreeString(), or by enabling automatic memory management.
 */
int RM_HashGet(RedisModuleKey *key, int flags, ...)
{
    va_list ap;
    if (key->value && key->value->type != OBJ_HASH)
        return REDISMODULE_ERR;

    va_start(ap, flags);
    while (1)
    {
        RedisModuleString *field, **valueptr;
        int *existsptr;
        /* Get the field object and the value pointer to pointer. */
        if (flags & REDISMODULE_HASH_CFIELDS)
        {
            char *cfield = va_arg(ap, char *);
            if (cfield == NULL)
                break;
            field = createRawStringObject(cfield, strlen(cfield));
        }
        else
        {
            field = va_arg(ap, RedisModuleString *);
            if (field == NULL)
                break;
        }

        /* Query the hash for existence or value object. */
        if (flags & REDISMODULE_HASH_EXISTS)
        {
            existsptr = va_arg(ap, int *);
            if (key->value)
                *existsptr = hashTypeExists(key->value, field->ptr);
            else
                *existsptr = 0;
        }
        else
        {
            valueptr = va_arg(ap, RedisModuleString **);
            if (key->value)
            {
                *valueptr = hashTypeGetValueObject(key->value, field->ptr);
                if (*valueptr)
                {
                    robj *decoded = getDecodedObject(*valueptr);
                    decrRefCount(*valueptr);
                    *valueptr = decoded;
                }
                if (*valueptr)
                    autoMemoryAdd(key->ctx, REDISMODULE_AM_STRING, *valueptr);
            }
            else
            {
                *valueptr = NULL;
            }
        }

        /* Cleanup */
        if (flags & REDISMODULE_HASH_CFIELDS)
            decrRefCount(field);
    }
    va_end(ap);
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Redis <-> Modules generic Call() API
 * -------------------------------------------------------------------------- */

/* Create a new RedisModuleCallReply object. The processing of the reply
 * is lazy, the object is just populated with the raw protocol and later
 * is processed as needed. Initially we just make sure to set the right
 * reply type, which is extremely cheap to do. */
RedisModuleCallReply *moduleCreateCallReplyFromProto(RedisModuleCtx *ctx, sds proto)
{
    RedisModuleCallReply *reply = zmalloc(sizeof(*reply));
    reply->ctx = ctx;
    reply->proto = proto;
    reply->protolen = sdslen(proto);
    reply->flags = REDISMODULE_REPLYFLAG_TOPARSE; /* Lazy parsing. */
    switch (proto[0])
    {
    case '$':
    case '+':
        reply->type = REDISMODULE_REPLY_STRING;
        break;
    case '-':
        reply->type = REDISMODULE_REPLY_ERROR;
        break;
    case ':':
        reply->type = REDISMODULE_REPLY_INTEGER;
        break;
    case '*':
        reply->type = REDISMODULE_REPLY_ARRAY;
        break;
    default:
        reply->type = REDISMODULE_REPLY_UNKNOWN;
        break;
    }
    if ((proto[0] == '*' || proto[0] == '$') && proto[1] == '-')
        reply->type = REDISMODULE_REPLY_NULL;
    return reply;
}

void moduleParseCallReply_Int(RedisModuleCallReply *reply);
void moduleParseCallReply_BulkString(RedisModuleCallReply *reply);
void moduleParseCallReply_SimpleString(RedisModuleCallReply *reply);
void moduleParseCallReply_Array(RedisModuleCallReply *reply);

/* Do nothing if REDISMODULE_REPLYFLAG_TOPARSE is false, otherwise
 * use the protcol of the reply in reply->proto in order to fill the
 * reply with parsed data according to the reply type. */
void moduleParseCallReply(RedisModuleCallReply *reply)
{
    if (!(reply->flags & REDISMODULE_REPLYFLAG_TOPARSE))
        return;
    reply->flags &= ~REDISMODULE_REPLYFLAG_TOPARSE;

    switch (reply->proto[0])
    {
    case ':':
        moduleParseCallReply_Int(reply);
        break;
    case '$':
        moduleParseCallReply_BulkString(reply);
        break;
    case '-': /* handled by next item. */
    case '+':
        moduleParseCallReply_SimpleString(reply);
        break;
    case '*':
        moduleParseCallReply_Array(reply);
        break;
    }
}

void moduleParseCallReply_Int(RedisModuleCallReply *reply)
{
    char *proto = reply->proto;
    char *p = strchr(proto + 1, '\r');

    string2ll(proto + 1, p - proto - 1, &reply->val.ll);
    reply->protolen = p - proto + 2;
    reply->type = REDISMODULE_REPLY_INTEGER;
}

void moduleParseCallReply_BulkString(RedisModuleCallReply *reply)
{
    char *proto = reply->proto;
    char *p = strchr(proto + 1, '\r');
    long long bulklen;

    string2ll(proto + 1, p - proto - 1, &bulklen);
    if (bulklen == -1)
    {
        reply->protolen = p - proto + 2;
        reply->type = REDISMODULE_REPLY_NULL;
    }
    else
    {
        reply->val.str = p + 2;
        reply->len = bulklen;
        reply->protolen = p - proto + 2 + bulklen + 2;
        reply->type = REDISMODULE_REPLY_STRING;
    }
}

void moduleParseCallReply_SimpleString(RedisModuleCallReply *reply)
{
    char *proto = reply->proto;
    char *p = strchr(proto + 1, '\r');

    reply->val.str = proto + 1;
    reply->len = p - proto - 1;
    reply->protolen = p - proto + 2;
    reply->type = proto[0] == '+' ? REDISMODULE_REPLY_STRING : REDISMODULE_REPLY_ERROR;
}

void moduleParseCallReply_Array(RedisModuleCallReply *reply)
{
    char *proto = reply->proto;
    char *p = strchr(proto + 1, '\r');
    long long arraylen, j;

    string2ll(proto + 1, p - proto - 1, &arraylen);
    p += 2;

    if (arraylen == -1)
    {
        reply->protolen = p - proto;
        reply->type = REDISMODULE_REPLY_NULL;
        return;
    }

    reply->val.array = zmalloc(sizeof(RedisModuleCallReply) * arraylen);
    reply->len = arraylen;
    for (j = 0; j < arraylen; j++)
    {
        RedisModuleCallReply *ele = reply->val.array + j;
        ele->flags = REDISMODULE_REPLYFLAG_NESTED |
                     REDISMODULE_REPLYFLAG_TOPARSE;
        ele->proto = p;
        ele->ctx = reply->ctx;
        moduleParseCallReply(ele);
        p += ele->protolen;
    }
    reply->protolen = p - proto;
    reply->type = REDISMODULE_REPLY_ARRAY;
}

/* Free a Call reply and all the nested replies it contains if it's an
 * array. */
void RM_FreeCallReply_Rec(RedisModuleCallReply *reply, int freenested)
{
    /* Don't free nested replies by default: the user must always free the
     * toplevel reply. However be gentle and don't crash if the module
     * misuses the API. */
    if (!freenested && reply->flags & REDISMODULE_REPLYFLAG_NESTED)
        return;

    if (!(reply->flags & REDISMODULE_REPLYFLAG_TOPARSE))
    {
        if (reply->type == REDISMODULE_REPLY_ARRAY)
        {
            size_t j;
            for (j = 0; j < reply->len; j++)
                RM_FreeCallReply_Rec(reply->val.array + j, 1);
            zfree(reply->val.array);
        }
    }

    /* For nested replies, we don't free reply->proto (which if not NULL
     * references the parent reply->proto buffer), nor the structure
     * itself which is allocated as an array of structures, and is freed
     * when the array value is released. */
    if (!(reply->flags & REDISMODULE_REPLYFLAG_NESTED))
    {
        if (reply->proto)
            sdsfree(reply->proto);
        zfree(reply);
    }
}

/* Wrapper for the recursive free reply function. This is needed in order
 * to have the first level function to return on nested replies, but only
 * if called by the module API. */
void RM_FreeCallReply(RedisModuleCallReply *reply)
{

    RedisModuleCtx *ctx = reply->ctx;
    RM_FreeCallReply_Rec(reply, 0);
    autoMemoryFreed(ctx, REDISMODULE_AM_REPLY, reply);
}

/* Return the reply type. */
int RM_CallReplyType(RedisModuleCallReply *reply)
{
    if (!reply)
        return REDISMODULE_REPLY_UNKNOWN;
    return reply->type;
}

/* Return the reply type length, where applicable. */
size_t RM_CallReplyLength(RedisModuleCallReply *reply)
{
    moduleParseCallReply(reply);
    switch (reply->type)
    {
    case REDISMODULE_REPLY_STRING:
    case REDISMODULE_REPLY_ERROR:
    case REDISMODULE_REPLY_ARRAY:
        return reply->len;
    default:
        return 0;
    }
}

/* Return the 'idx'-th nested call reply element of an array reply, or NULL
 * if the reply type is wrong or the index is out of range. */
RedisModuleCallReply *RM_CallReplyArrayElement(RedisModuleCallReply *reply, size_t idx)
{
    moduleParseCallReply(reply);
    if (reply->type != REDISMODULE_REPLY_ARRAY)
        return NULL;
    if (idx >= reply->len)
        return NULL;
    return reply->val.array + idx;
}

/* Return the long long of an integer reply. */
long long RM_CallReplyInteger(RedisModuleCallReply *reply)
{
    moduleParseCallReply(reply);
    if (reply->type != REDISMODULE_REPLY_INTEGER)
        return LLONG_MIN;
    return reply->val.ll;
}

/* Return the pointer and length of a string or error reply. */
const char *RM_CallReplyStringPtr(RedisModuleCallReply *reply, size_t *len)
{
    moduleParseCallReply(reply);
    if (reply->type != REDISMODULE_REPLY_STRING &&
        reply->type != REDISMODULE_REPLY_ERROR)
        return NULL;
    if (len)
        *len = reply->len;
    return reply->val.str;
}

/* Return a new string object from a call reply of type string, error or
 * integer. Otherwise (wrong reply type) return NULL. */
RedisModuleString *RM_CreateStringFromCallReply(RedisModuleCallReply *reply)
{
    moduleParseCallReply(reply);
    switch (reply->type)
    {
    case REDISMODULE_REPLY_STRING:
    case REDISMODULE_REPLY_ERROR:
        return RM_CreateString(reply->ctx, reply->val.str, reply->len);
    case REDISMODULE_REPLY_INTEGER:
    {
        char buf[64];
        int len = ll2string(buf, sizeof(buf), reply->val.ll);
        return RM_CreateString(reply->ctx, buf, len);
    }
    default:
        return NULL;
    }
}

/* Returns an array of robj pointers, and populates *argc with the number
 * of items, by parsing the format specifier "fmt" as described for
 * the RM_Call(), RM_Replicate() and other module APIs.
 *
 * The integer pointed by 'flags' is populated with flags according
 * to special modifiers in "fmt". For now only one exists:
 *
 *     "!" -> REDISMODULE_ARGV_REPLICATE
 *     "A" -> REDISMODULE_ARGV_NO_AOF
 *     "R" -> REDISMODULE_ARGV_NO_REPLICAS
 *
 * On error (format specifier error) NULL is returned and nothing is
 * allocated. On success the argument vector is returned. */
robj **moduleCreateArgvFromUserFormat(const char *cmdname, const char *fmt, int *argcp, int *flags, va_list ap)
{
    int argc = 0, argv_size, j;
    robj **argv = NULL;

    /* As a first guess to avoid useless reallocations, size argv to
     * hold one argument for each char specifier in 'fmt'. */
    argv_size = strlen(fmt) + 1; /* +1 because of the command name. */
    argv = zrealloc(argv, sizeof(robj *) * argv_size);

    /* Build the arguments vector based on the format specifier. */
    argv[0] = createStringObject(cmdname, strlen(cmdname));
    argc++;

    /* Create the client and dispatch the command. */
    const char *p = fmt;
    while (*p)
    {
        if (*p == 'c')
        {
            char *cstr = va_arg(ap, char *);
            argv[argc++] = createStringObject(cstr, strlen(cstr));
        }
        else if (*p == 's')
        {
            robj *obj = va_arg(ap, void *);
            argv[argc++] = obj;
            incrRefCount(obj);
        }
        else if (*p == 'b')
        {
            char *buf = va_arg(ap, char *);
            size_t len = va_arg(ap, size_t);
            argv[argc++] = createStringObject(buf, len);
        }
        else if (*p == 'l')
        {
            long long ll = va_arg(ap, long long);
            argv[argc++] = createObject(OBJ_STRING, sdsfromlonglong(ll));
        }
        else if (*p == 'v')
        {
            /* A vector of strings */
            robj **v = va_arg(ap, void *);
            size_t vlen = va_arg(ap, size_t);

            /* We need to grow argv to hold the vector's elements.
             * We resize by vector_len-1 elements, because we held
             * one element in argv for the vector already */
            argv_size += vlen - 1;
            argv = zrealloc(argv, sizeof(robj *) * argv_size);

            size_t i = 0;
            for (i = 0; i < vlen; i++)
            {
                incrRefCount(v[i]);
                argv[argc++] = v[i];
            }
        }
        else if (*p == '!')
        {
            if (flags)
                (*flags) |= REDISMODULE_ARGV_REPLICATE;
        }
        else if (*p == 'A')
        {
            if (flags)
                (*flags) |= REDISMODULE_ARGV_NO_AOF;
        }
        else if (*p == 'R')
        {
            if (flags)
                (*flags) |= REDISMODULE_ARGV_NO_REPLICAS;
        }
        else
        {
            goto fmterr;
        }
        p++;
    }
    *argcp = argc;
    return argv;

fmterr:
    for (j = 0; j < argc; j++)
        decrRefCount(argv[j]);
    zfree(argv);
    return NULL;
}

/* Exported API to call any Redis command from modules.
 * On success a RedisModuleCallReply object is returned, otherwise
 * NULL is returned and errno is set to the following values:
 *
 * EINVAL: command non existing, wrong arity, wrong format specifier.
 * EPERM:  operation in Cluster instance with key in non local slot.
 *
 * This API is documented here: https://redis.io/topics/modules-intro
 */
RedisModuleCallReply *RM_Call(RedisModuleCtx *ctx, const char *cmdname, const char *fmt, ...)
{
    struct redisCommand *cmd;
    client *c = NULL;
    robj **argv = NULL;
    int argc = 0, flags = 0;
    va_list ap;
    RedisModuleCallReply *reply = NULL;
    int replicate = 0; /* Replicate this command? */

    /* Create the client and dispatch the command. */
    va_start(ap, fmt);
    c = createClient(-1);
    argv = moduleCreateArgvFromUserFormat(cmdname, fmt, &argc, &flags, ap);
    replicate = flags & REDISMODULE_ARGV_REPLICATE;
    va_end(ap);

    /* Setup our fake client for command execution. */
    c->flags |= CLIENT_MODULE;
    c->db = ctx->client->db;
    c->argv = argv;
    c->argc = argc;
    if (ctx->module)
        ctx->module->in_call++;

    /* We handle the above format error only when the client is setup so that
     * we can free it normally. */
    if (argv == NULL)
        goto cleanup;

    /* Call command filters */
    moduleCallCommandFilters(c);

    /* Lookup command now, after filters had a chance to make modifications
     * if necessary.
     */
    cmd = lookupCommand(c->argv[0]->ptr);
    if (!cmd)
    {
        errno = EINVAL;
        goto cleanup;
    }
    c->cmd = c->lastcmd = cmd;

    /* Basic arity checks. */
    if ((cmd->arity > 0 && cmd->arity != argc) || (argc < -cmd->arity))
    {
        errno = EINVAL;
        goto cleanup;
    }

    /* If this is a Redis Cluster node, we need to make sure the module is not
     * trying to access non-local keys, with the exception of commands
     * received from our master. */
    if (server.cluster_enabled && !(ctx->client->flags & CLIENT_MASTER))
    {
        /* Duplicate relevant flags in the module client. */
        c->flags &= ~(CLIENT_READONLY | CLIENT_ASKING);
        c->flags |= ctx->client->flags & (CLIENT_READONLY | CLIENT_ASKING);
        if (getNodeByQuery(c, c->cmd, c->argv, c->argc, NULL, NULL) !=
            server.cluster->myself)
        {
            errno = EPERM;
            goto cleanup;
        }
    }

    /* If we are using single commands replication, we need to wrap what
     * we propagate into a MULTI/EXEC block, so that it will be atomic like
     * a Lua script in the context of AOF and slaves. */
    if (replicate)
        moduleReplicateMultiIfNeeded(ctx);

    /* Run the command */
    int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS;
    if (replicate)
    {
        if (!(flags & REDISMODULE_ARGV_NO_AOF))
            call_flags |= CMD_CALL_PROPAGATE_AOF;
        if (!(flags & REDISMODULE_ARGV_NO_REPLICAS))
            call_flags |= CMD_CALL_PROPAGATE_REPL;
    }
    call(c, call_flags);

    /* Convert the result of the Redis command into a suitable Lua type.
     * The first thing we need is to create a single string from the client
     * output buffers. */
    sds proto = sdsnewlen(c->buf, c->bufpos);
    c->bufpos = 0;
    while (listLength(c->reply))
    {
        clientReplyBlock *o = listNodeValue(listFirst(c->reply));

        proto = sdscatlen(proto, o->buf, o->used);
        listDelNode(c->reply, listFirst(c->reply));
    }
    reply = moduleCreateCallReplyFromProto(ctx, proto);
    autoMemoryAdd(ctx, REDISMODULE_AM_REPLY, reply);

cleanup:
    if (ctx->module)
        ctx->module->in_call--;
    freeClient(c);
    return reply;
}

/* Return a pointer, and a length, to the protocol returned by the command
 * that returned the reply object. */
const char *RM_CallReplyProto(RedisModuleCallReply *reply, size_t *len)
{
    if (reply->proto)
        *len = sdslen(reply->proto);
    return reply->proto;
}

/* --------------------------------------------------------------------------
 * Modules data types
 *
 * When String DMA or using existing data structures is not enough, it is
 * possible to create new data types from scratch and export them to
 * Redis. The module must provide a set of callbacks for handling the
 * new values exported (for example in order to provide RDB saving/loading,
 * AOF rewrite, and so forth). In this section we define this API.
 * -------------------------------------------------------------------------- */

/* Turn a 9 chars name in the specified charset and a 10 bit encver into
 * a single 64 bit unsigned integer that represents this exact module name
 * and version. This final number is called a "type ID" and is used when
 * writing module exported values to RDB files, in order to re-associate the
 * value to the right module to load them during RDB loading.
 *
 * If the string is not of the right length or the charset is wrong, or
 * if encver is outside the unsigned 10 bit integer range, 0 is returned,
 * otherwise the function returns the right type ID.
 *
 * The resulting 64 bit integer is composed as follows:
 *
 *     (high order bits) 6|6|6|6|6|6|6|6|6|10 (low order bits)
 *
 * The first 6 bits value is the first character, name[0], while the last
 * 6 bits value, immediately before the 10 bits integer, is name[8].
 * The last 10 bits are the encoding version.
 *
 * Note that a name and encver combo of "AAAAAAAAA" and 0, will produce
 * zero as return value, that is the same we use to signal errors, thus
 * this combination is invalid, and also useless since type names should
 * try to be vary to avoid collisions. */

const char *ModuleTypeNameCharSet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789-_";

uint64_t moduleTypeEncodeId(const char *name, int encver)
{
    /* We use 64 symbols so that we can map each character into 6 bits
     * of the final output. */
    const char *cset = ModuleTypeNameCharSet;
    if (strlen(name) != 9)
        return 0;
    if (encver < 0 || encver > 1023)
        return 0;

    uint64_t id = 0;
    for (int j = 0; j < 9; j++)
    {
        char *p = strchr(cset, name[j]);
        if (!p)
            return 0;
        unsigned long pos = p - cset;
        id = (id << 6) | pos;
    }
    id = (id << 10) | encver;
    return id;
}

/* Search, in the list of exported data types of all the modules registered,
 * a type with the same name as the one given. Returns the moduleType
 * structure pointer if such a module is found, or NULL otherwise. */
moduleType *moduleTypeLookupModuleByName(const char *name)
{
    dictIterator *di = dictGetIterator(modules);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL)
    {
        struct RedisModule *module = dictGetVal(de);
        listIter li;
        listNode *ln;

        listRewind(module->types, &li);
        while ((ln = listNext(&li)))
        {
            moduleType *mt = ln->value;
            if (memcmp(name, mt->name, sizeof(mt->name)) == 0)
            {
                dictReleaseIterator(di);
                return mt;
            }
        }
    }
    dictReleaseIterator(di);
    return NULL;
}

/* Lookup a module by ID, with caching. This function is used during RDB
 * loading. Modules exporting data types should never be able to unload, so
 * our cache does not need to expire. */
#define MODULE_LOOKUP_CACHE_SIZE 3

moduleType *moduleTypeLookupModuleByID(uint64_t id)
{
    static struct
    {
        uint64_t id;
        moduleType *mt;
    } cache[MODULE_LOOKUP_CACHE_SIZE];

    /* Search in cache to start. */
    int j;
    for (j = 0; j < MODULE_LOOKUP_CACHE_SIZE && cache[j].mt != NULL; j++)
        if (cache[j].id == id)
            return cache[j].mt;

    /* Slow module by module lookup. */
    moduleType *mt = NULL;
    dictIterator *di = dictGetIterator(modules);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL && mt == NULL)
    {
        struct RedisModule *module = dictGetVal(de);
        listIter li;
        listNode *ln;

        listRewind(module->types, &li);
        while ((ln = listNext(&li)))
        {
            moduleType *this_mt = ln->value;
            /* Compare only the 54 bit module identifier and not the
             * encoding version. */
            if (this_mt->id >> 10 == id >> 10)
            {
                mt = this_mt;
                break;
            }
        }
    }
    dictReleaseIterator(di);

    /* Add to cache if possible. */
    if (mt && j < MODULE_LOOKUP_CACHE_SIZE)
    {
        cache[j].id = id;
        cache[j].mt = mt;
    }
    return mt;
}

/* Turn an (unresolved) module ID into a type name, to show the user an
 * error when RDB files contain module data we can't load.
 * The buffer pointed by 'name' must be 10 bytes at least. The function will
 * fill it with a null terminated module name. */
void moduleTypeNameByID(char *name, uint64_t moduleid)
{
    const char *cset = ModuleTypeNameCharSet;

    name[9] = '\0';
    char *p = name + 8;
    moduleid >>= 10;
    for (int j = 0; j < 9; j++)
    {
        *p-- = cset[moduleid & 63];
        moduleid >>= 6;
    }
}

/* Register a new data type exported by the module. The parameters are the
 * following. Please for in depth documentation check the modules API
 * documentation, especially the TYPES.md file.
 *
 * * **name**: A 9 characters data type name that MUST be unique in the Redis
 *   Modules ecosystem. Be creative... and there will be no collisions. Use
 *   the charset A-Z a-z 9-0, plus the two "-_" characters. A good
 *   idea is to use, for example `<typename>-<vendor>`. For example
 *   "tree-AntZ" may mean "Tree data structure by @antirez". To use both
 *   lower case and upper case letters helps in order to prevent collisions.
 * * **encver**: Encoding version, which is, the version of the serialization
 *   that a module used in order to persist data. As long as the "name"
 *   matches, the RDB loading will be dispatched to the type callbacks
 *   whatever 'encver' is used, however the module can understand if
 *   the encoding it must load are of an older version of the module.
 *   For example the module "tree-AntZ" initially used encver=0. Later
 *   after an upgrade, it started to serialize data in a different format
 *   and to register the type with encver=1. However this module may
 *   still load old data produced by an older version if the rdb_load
 *   callback is able to check the encver value and act accordingly.
 *   The encver must be a positive value between 0 and 1023.
 * * **typemethods_ptr** is a pointer to a RedisModuleTypeMethods structure
 *   that should be populated with the methods callbacks and structure
 *   version, like in the following example:
 *
 *      RedisModuleTypeMethods tm = {
 *          .version = REDISMODULE_TYPE_METHOD_VERSION,
 *          .rdb_load = myType_RDBLoadCallBack,
 *          .rdb_save = myType_RDBSaveCallBack,
 *          .aof_rewrite = myType_AOFRewriteCallBack,
 *          .free = myType_FreeCallBack,
 *
 *          // Optional fields
 *          .digest = myType_DigestCallBack,
 *          .mem_usage = myType_MemUsageCallBack,
 *      }
 *
 * * **rdb_load**: A callback function pointer that loads data from RDB files.
 * * **rdb_save**: A callback function pointer that saves data to RDB files.
 * * **aof_rewrite**: A callback function pointer that rewrites data as commands.
 * * **digest**: A callback function pointer that is used for `DEBUG DIGEST`.
 * * **free**: A callback function pointer that can free a type value.
 *
 * The **digest* and **mem_usage** methods should currently be omitted since
 * they are not yet implemented inside the Redis modules core.
 *
 * Note: the module name "AAAAAAAAA" is reserved and produces an error, it
 * happens to be pretty lame as well.
 *
 * If there is already a module registering a type with the same name,
 * and if the module name or encver is invalid, NULL is returned.
 * Otherwise the new type is registered into Redis, and a reference of
 * type RedisModuleType is returned: the caller of the function should store
 * this reference into a gobal variable to make future use of it in the
 * modules type API, since a single module may register multiple types.
 * Example code fragment:
 *
 *      static RedisModuleType *BalancedTreeType;
 *
 *      int RedisModule_OnLoad(RedisModuleCtx *ctx) {
 *          // some code here ...
 *          BalancedTreeType = RM_CreateDataType(...);
 *      }
 */
moduleType *RM_CreateDataType(RedisModuleCtx *ctx, const char *name, int encver, void *typemethods_ptr)
{
    uint64_t id = moduleTypeEncodeId(name, encver);
    if (id == 0)
        return NULL;
    if (moduleTypeLookupModuleByName(name) != NULL)
        return NULL;

    long typemethods_version = ((long *)typemethods_ptr)[0];
    if (typemethods_version == 0)
        return NULL;

    struct typemethods
    {
        uint64_t version;
        moduleTypeLoadFunc rdb_load;
        moduleTypeSaveFunc rdb_save;
        moduleTypeRewriteFunc aof_rewrite;
        moduleTypeMemUsageFunc mem_usage;
        moduleTypeDigestFunc digest;
        moduleTypeFreeFunc free;
        struct
        {
            moduleTypeAuxLoadFunc aux_load;
            moduleTypeAuxSaveFunc aux_save;
            int aux_save_triggers;
        } v2;
    } *tms = (struct typemethods *)typemethods_ptr;

    moduleType *mt = zcalloc(sizeof(*mt));
    mt->id = id;
    mt->module = ctx->module;
    mt->rdb_load = tms->rdb_load;
    mt->rdb_save = tms->rdb_save;
    mt->aof_rewrite = tms->aof_rewrite;
    mt->mem_usage = tms->mem_usage;
    mt->digest = tms->digest;
    mt->free = tms->free;
    if (tms->version >= 2)
    {
        mt->aux_load = tms->v2.aux_load;
        mt->aux_save = tms->v2.aux_save;
        mt->aux_save_triggers = tms->v2.aux_save_triggers;
    }
    memcpy(mt->name, name, sizeof(mt->name));
    listAddNodeTail(ctx->module->types, mt);
    return mt;
}

/* If the key is open for writing, set the specified module type object
 * as the value of the key, deleting the old value if any.
 * On success REDISMODULE_OK is returned. If the key is not open for
 * writing or there is an active iterator, REDISMODULE_ERR is returned. */
int RM_ModuleTypeSetValue(RedisModuleKey *key, moduleType *mt, void *value)
{
    if (!(key->mode & REDISMODULE_WRITE) || key->iter)
        return REDISMODULE_ERR;
    RM_DeleteKey(key);
    robj *o = createModuleObject(mt, value);
    setKey(key->db, key->key, o);
    decrRefCount(o);
    key->value = o;
    return REDISMODULE_OK;
}

/* Assuming RedisModule_KeyType() returned REDISMODULE_KEYTYPE_MODULE on
 * the key, returns the module type pointer of the value stored at key.
 *
 * If the key is NULL, is not associated with a module type, or is empty,
 * then NULL is returned instead. */
moduleType *RM_ModuleTypeGetType(RedisModuleKey *key)
{
    if (key == NULL ||
        key->value == NULL ||
        RM_KeyType(key) != REDISMODULE_KEYTYPE_MODULE)
        return NULL;
    moduleValue *mv = key->value->ptr;
    return mv->type;
}

/* Assuming RedisModule_KeyType() returned REDISMODULE_KEYTYPE_MODULE on
 * the key, returns the module type low-level value stored at key, as
 * it was set by the user via RedisModule_ModuleTypeSet().
 *
 * If the key is NULL, is not associated with a module type, or is empty,
 * then NULL is returned instead. */
void *RM_ModuleTypeGetValue(RedisModuleKey *key)
{
    if (key == NULL ||
        key->value == NULL ||
        RM_KeyType(key) != REDISMODULE_KEYTYPE_MODULE)
        return NULL;
    moduleValue *mv = key->value->ptr;
    return mv->value;
}

/* --------------------------------------------------------------------------
 * RDB loading and saving functions
 * -------------------------------------------------------------------------- */

/* Called when there is a load error in the context of a module. This cannot
 * be recovered like for the built-in types. */
void moduleRDBLoadError(RedisModuleIO *io)
{
    serverLog(LL_WARNING,
              "Error loading data from RDB (short read or EOF). "
              "Read performed by module '%s' about type '%s' "
              "after reading '%llu' bytes of a value.",
              io->type->module->name,
              io->type->name,
              (unsigned long long)io->bytes);
    exit(1);
}

/* Save an unsigned 64 bit value into the RDB file. This function should only
 * be called in the context of the rdb_save method of modules implementing new
 * data types. */
void RM_SaveUnsigned(RedisModuleIO *io, uint64_t value)
{
    if (io->error)
        return;
    /* Save opcode. */
    int retval = rdbSaveLen(io->rio, RDB_MODULE_OPCODE_UINT);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    /* Save value. */
    retval = rdbSaveLen(io->rio, value);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    return;

saveerr:
    io->error = 1;
}

/* Load an unsigned 64 bit value from the RDB file. This function should only
 * be called in the context of the rdb_load method of modules implementing
 * new data types. */
uint64_t RM_LoadUnsigned(RedisModuleIO *io)
{
    if (io->ver == 2)
    {
        uint64_t opcode = rdbLoadLen(io->rio, NULL);
        if (opcode != RDB_MODULE_OPCODE_UINT)
            goto loaderr;
    }
    uint64_t value;
    int retval = rdbLoadLenByRef(io->rio, NULL, &value);
    if (retval == -1)
        goto loaderr;
    return value;

loaderr:
    moduleRDBLoadError(io);
    return 0; /* Never reached. */
}

/* Like RedisModule_SaveUnsigned() but for signed 64 bit values. */
void RM_SaveSigned(RedisModuleIO *io, int64_t value)
{
    union
    {
        uint64_t u;
        int64_t i;
    } conv;
    conv.i = value;
    RM_SaveUnsigned(io, conv.u);
}

/* Like RedisModule_LoadUnsigned() but for signed 64 bit values. */
int64_t RM_LoadSigned(RedisModuleIO *io)
{
    union
    {
        uint64_t u;
        int64_t i;
    } conv;
    conv.u = RM_LoadUnsigned(io);
    return conv.i;
}

/* In the context of the rdb_save method of a module type, saves a
 * string into the RDB file taking as input a RedisModuleString.
 *
 * The string can be later loaded with RedisModule_LoadString() or
 * other Load family functions expecting a serialized string inside
 * the RDB file. */
void RM_SaveString(RedisModuleIO *io, RedisModuleString *s)
{
    if (io->error)
        return;
    /* Save opcode. */
    ssize_t retval = rdbSaveLen(io->rio, RDB_MODULE_OPCODE_STRING);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    /* Save value. */
    retval = rdbSaveStringObject(io->rio, s);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    return;

saveerr:
    io->error = 1;
}

/* Like RedisModule_SaveString() but takes a raw C pointer and length
 * as input. */
void RM_SaveStringBuffer(RedisModuleIO *io, const char *str, size_t len)
{
    if (io->error)
        return;
    /* Save opcode. */
    ssize_t retval = rdbSaveLen(io->rio, RDB_MODULE_OPCODE_STRING);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    /* Save value. */
    retval = rdbSaveRawString(io->rio, (unsigned char *)str, len);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    return;

saveerr:
    io->error = 1;
}

/* Implements RM_LoadString() and RM_LoadStringBuffer() */
void *moduleLoadString(RedisModuleIO *io, int plain, size_t *lenptr)
{
    if (io->ver == 2)
    {
        uint64_t opcode = rdbLoadLen(io->rio, NULL);
        if (opcode != RDB_MODULE_OPCODE_STRING)
            goto loaderr;
    }
    void *s = rdbGenericLoadStringObject(io->rio,
                                         plain ? RDB_LOAD_PLAIN : RDB_LOAD_NONE, lenptr);
    if (s == NULL)
        goto loaderr;
    return s;

loaderr:
    moduleRDBLoadError(io);
    return NULL; /* Never reached. */
}

/* In the context of the rdb_load method of a module data type, loads a string
 * from the RDB file, that was previously saved with RedisModule_SaveString()
 * functions family.
 *
 * The returned string is a newly allocated RedisModuleString object, and
 * the user should at some point free it with a call to RedisModule_FreeString().
 *
 * If the data structure does not store strings as RedisModuleString objects,
 * the similar function RedisModule_LoadStringBuffer() could be used instead. */
RedisModuleString *RM_LoadString(RedisModuleIO *io)
{
    return moduleLoadString(io, 0, NULL);
}

/* Like RedisModule_LoadString() but returns an heap allocated string that
 * was allocated with RedisModule_Alloc(), and can be resized or freed with
 * RedisModule_Realloc() or RedisModule_Free().
 *
 * The size of the string is stored at '*lenptr' if not NULL.
 * The returned string is not automatically NULL termianted, it is loaded
 * exactly as it was stored inisde the RDB file. */
char *RM_LoadStringBuffer(RedisModuleIO *io, size_t *lenptr)
{
    return moduleLoadString(io, 1, lenptr);
}

/* In the context of the rdb_save method of a module data type, saves a double
 * value to the RDB file. The double can be a valid number, a NaN or infinity.
 * It is possible to load back the value with RedisModule_LoadDouble(). */
void RM_SaveDouble(RedisModuleIO *io, double value)
{
    if (io->error)
        return;
    /* Save opcode. */
    int retval = rdbSaveLen(io->rio, RDB_MODULE_OPCODE_DOUBLE);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    /* Save value. */
    retval = rdbSaveBinaryDoubleValue(io->rio, value);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    return;

saveerr:
    io->error = 1;
}

/* In the context of the rdb_save method of a module data type, loads back the
 * double value saved by RedisModule_SaveDouble(). */
double RM_LoadDouble(RedisModuleIO *io)
{
    if (io->ver == 2)
    {
        uint64_t opcode = rdbLoadLen(io->rio, NULL);
        if (opcode != RDB_MODULE_OPCODE_DOUBLE)
            goto loaderr;
    }
    double value;
    int retval = rdbLoadBinaryDoubleValue(io->rio, &value);
    if (retval == -1)
        goto loaderr;
    return value;

loaderr:
    moduleRDBLoadError(io);
    return 0; /* Never reached. */
}

/* In the context of the rdb_save method of a module data type, saves a float
 * value to the RDB file. The float can be a valid number, a NaN or infinity.
 * It is possible to load back the value with RedisModule_LoadFloat(). */
void RM_SaveFloat(RedisModuleIO *io, float value)
{
    if (io->error)
        return;
    /* Save opcode. */
    int retval = rdbSaveLen(io->rio, RDB_MODULE_OPCODE_FLOAT);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    /* Save value. */
    retval = rdbSaveBinaryFloatValue(io->rio, value);
    if (retval == -1)
        goto saveerr;
    io->bytes += retval;
    return;

saveerr:
    io->error = 1;
}

/* In the context of the rdb_save method of a module data type, loads back the
 * float value saved by RedisModule_SaveFloat(). */
float RM_LoadFloat(RedisModuleIO *io)
{
    if (io->ver == 2)
    {
        uint64_t opcode = rdbLoadLen(io->rio, NULL);
        if (opcode != RDB_MODULE_OPCODE_FLOAT)
            goto loaderr;
    }
    float value;
    int retval = rdbLoadBinaryFloatValue(io->rio, &value);
    if (retval == -1)
        goto loaderr;
    return value;

loaderr:
    moduleRDBLoadError(io);
    return 0; /* Never reached. */
}

/* Iterate over modules, and trigger rdb aux saving for the ones modules types
 * who asked for it. */
ssize_t rdbSaveModulesAux(rio *rdb, int when)
{
    size_t total_written = 0;
    dictIterator *di = dictGetIterator(modules);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL)
    {
        struct RedisModule *module = dictGetVal(de);
        listIter li;
        listNode *ln;

        listRewind(module->types, &li);
        while ((ln = listNext(&li)))
        {
            moduleType *mt = ln->value;
            if (!mt->aux_save || !(mt->aux_save_triggers & when))
                continue;
            ssize_t ret = rdbSaveSingleModuleAux(rdb, when, mt);
            if (ret == -1)
            {
                dictReleaseIterator(di);
                return -1;
            }
            total_written += ret;
        }
    }

    dictReleaseIterator(di);
    return total_written;
}

/* --------------------------------------------------------------------------
 * Key digest API (DEBUG DIGEST interface for modules types)
 * -------------------------------------------------------------------------- */

/* Add a new element to the digest. This function can be called multiple times
 * one element after the other, for all the elements that constitute a given
 * data structure. The function call must be followed by the call to
 * `RedisModule_DigestEndSequence` eventually, when all the elements that are
 * always in a given order are added. See the Redis Modules data types
 * documentation for more info. However this is a quick example that uses Redis
 * data types as an example.
 *
 * To add a sequence of unordered elements (for example in the case of a Redis
 * Set), the pattern to use is:
 *
 *     foreach element {
 *         AddElement(element);
 *         EndSequence();
 *     }
 *
 * Because Sets are not ordered, so every element added has a position that
 * does not depend from the other. However if instead our elements are
 * ordered in pairs, like field-value pairs of an Hash, then one should
 * use:
 *
 *     foreach key,value {
 *         AddElement(key);
 *         AddElement(value);
 *         EndSquence();
 *     }
 *
 * Because the key and value will be always in the above order, while instead
 * the single key-value pairs, can appear in any position into a Redis hash.
 *
 * A list of ordered elements would be implemented with:
 *
 *     foreach element {
 *         AddElement(element);
 *     }
 *     EndSequence();
 *
 */
void RM_DigestAddStringBuffer(RedisModuleDigest *md, unsigned char *ele, size_t len)
{
    mixDigest(md->o, ele, len);
}

/* Like `RedisModule_DigestAddStringBuffer()` but takes a long long as input
 * that gets converted into a string before adding it to the digest. */
void RM_DigestAddLongLong(RedisModuleDigest *md, long long ll)
{
    char buf[LONG_STR_SIZE];
    size_t len = ll2string(buf, sizeof(buf), ll);
    mixDigest(md->o, buf, len);
}

/* See the documentation for `RedisModule_DigestAddElement()`. */
void RM_DigestEndSequence(RedisModuleDigest *md)
{
    xorDigest(md->x, md->o, sizeof(md->o));
    memset(md->o, 0, sizeof(md->o));
}

/* --------------------------------------------------------------------------
 * AOF API for modules data types
 * -------------------------------------------------------------------------- */

/* Emits a command into the AOF during the AOF rewriting process. This function
 * is only called in the context of the aof_rewrite method of data types exported
 * by a module. The command works exactly like RedisModule_Call() in the way
 * the parameters are passed, but it does not return anything as the error
 * handling is performed by Redis itself. */
void RM_EmitAOF(RedisModuleIO *io, const char *cmdname, const char *fmt, ...)
{
    if (io->error)
        return;
    struct redisCommand *cmd;
    robj **argv = NULL;
    int argc = 0, flags = 0, j;
    va_list ap;

    cmd = lookupCommandByCString((char *)cmdname);
    if (!cmd)
    {
        serverLog(LL_WARNING,
                  "Fatal: AOF method for module data type '%s' tried to "
                  "emit unknown command '%s'",
                  io->type->name, cmdname);
        io->error = 1;
        errno = EINVAL;
        return;
    }

    /* Emit the arguments into the AOF in Redis protocol format. */
    va_start(ap, fmt);
    argv = moduleCreateArgvFromUserFormat(cmdname, fmt, &argc, &flags, ap);
    va_end(ap);
    if (argv == NULL)
    {
        serverLog(LL_WARNING,
                  "Fatal: AOF method for module data type '%s' tried to "
                  "call RedisModule_EmitAOF() with wrong format specifiers '%s'",
                  io->type->name, fmt);
        io->error = 1;
        errno = EINVAL;
        return;
    }

    /* Bulk count. */
    if (!io->error && rioWriteBulkCount(io->rio, '*', argc) == 0)
        io->error = 1;

    /* Arguments. */
    for (j = 0; j < argc; j++)
    {
        if (!io->error && rioWriteBulkObject(io->rio, argv[j]) == 0)
            io->error = 1;
        decrRefCount(argv[j]);
    }
    zfree(argv);
    return;
}

/* --------------------------------------------------------------------------
 * IO context handling
 * -------------------------------------------------------------------------- */

RedisModuleCtx *RM_GetContextFromIO(RedisModuleIO *io)
{
    if (io->ctx)
        return io->ctx; /* Can't have more than one... */
    RedisModuleCtx ctxtemplate = REDISMODULE_CTX_INIT;
    io->ctx = zmalloc(sizeof(RedisModuleCtx));
    *(io->ctx) = ctxtemplate;
    io->ctx->module = io->type->module;
    io->ctx->client = NULL;
    return io->ctx;
}

/* Returns a RedisModuleString with the name of the key currently saving or
 * loading, when an IO data type callback is called.  There is no guarantee
 * that the key name is always available, so this may return NULL.
 */
const RedisModuleString *RM_GetKeyNameFromIO(RedisModuleIO *io)
{
    return io->key;
}

/* --------------------------------------------------------------------------
 * Logging
 * -------------------------------------------------------------------------- */

/* This is the low level function implementing both:
 *
 *      RM_Log()
 *      RM_LogIOError()
 *
 */
void RM_LogRaw(RedisModule *module, const char *levelstr, const char *fmt, va_list ap)
{
    char msg[LOG_MAX_LEN];
    size_t name_len;
    int level;

    if (!strcasecmp(levelstr, "debug"))
        level = LL_DEBUG;
    else if (!strcasecmp(levelstr, "verbose"))
        level = LL_VERBOSE;
    else if (!strcasecmp(levelstr, "notice"))
        level = LL_NOTICE;
    else if (!strcasecmp(levelstr, "warning"))
        level = LL_WARNING;
    else
        level = LL_VERBOSE; /* Default. */

    if (level < server.verbosity)
        return;

    name_len = snprintf(msg, sizeof(msg), "<%s> ", module ? module->name : "module");
    vsnprintf(msg + name_len, sizeof(msg) - name_len, fmt, ap);
    serverLogRaw(level, msg);
}

/* Produces a log message to the standard Redis log, the format accepts
 * printf-alike specifiers, while level is a string describing the log
 * level to use when emitting the log, and must be one of the following:
 *
 * * "debug"
 * * "verbose"
 * * "notice"
 * * "warning"
 *
 * If the specified log level is invalid, verbose is used by default.
 * There is a fixed limit to the length of the log line this function is able
 * to emit, this limit is not specified but is guaranteed to be more than
 * a few lines of text.
 *
 * The ctx argument may be NULL if cannot be provided in the context of the
 * caller for instance threads or callbacks, in which case a generic "module"
 * will be used instead of the module name.
 */
void RM_Log(RedisModuleCtx *ctx, const char *levelstr, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    RM_LogRaw(ctx ? ctx->module : NULL, levelstr, fmt, ap);
    va_end(ap);
}

/* Log errors from RDB / AOF serialization callbacks.
 *
 * This function should be used when a callback is returning a critical
 * error to the caller since cannot load or save the data for some
 * critical reason. */
void RM_LogIOError(RedisModuleIO *io, const char *levelstr, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    RM_LogRaw(io->type->module, levelstr, fmt, ap);
    va_end(ap);
}

/* --------------------------------------------------------------------------
 * Blocking clients from modules
 * -------------------------------------------------------------------------- */

/* Readable handler for the awake pipe. We do nothing here, the awake bytes
 * will be actually read in a more appropriate place in the
 * moduleHandleBlockedClients() function that is where clients are actually
 * served. */
void moduleBlockedClientPipeReadable(aeEventLoop *el, int fd, void *privdata, int mask)
{
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    UNUSED(privdata);
}

/* This is called from blocked.c in order to unblock a client: may be called
 * for multiple reasons while the client is in the middle of being blocked
 * because the client is terminated, but is also called for cleanup when a
 * client is unblocked in a clean way after replaying.
 *
 * What we do here is just to set the client to NULL in the redis module
 * blocked client handle. This way if the client is terminated while there
 * is a pending threaded operation involving the blocked client, we'll know
 * that the client no longer exists and no reply callback should be called.
 *
 * The structure RedisModuleBlockedClient will be always deallocated when
 * running the list of clients blocked by a module that need to be unblocked. */
void unblockClientFromModule(client *c)
{
    RedisModuleBlockedClient *bc = c->bpop.module_blocked_handle;

    /* Call the disconnection callback if any. */
    if (bc->disconnect_callback)
    {
        RedisModuleCtx ctx = REDISMODULE_CTX_INIT;
        ctx.blocked_privdata = bc->privdata;
        ctx.module = bc->module;
        ctx.client = bc->client;
        bc->disconnect_callback(&ctx, bc);
        moduleFreeContext(&ctx);
    }

    bc->client = NULL;
    /* Reset the client for a new query since, for blocking commands implemented
     * into modules, we do not it immediately after the command returns (and
     * the client blocks) in order to be still able to access the argument
     * vector from callbacks. */
    resetClient(c);
}

/* Block a client in the context of a blocking command, returning an handle
 * which will be used, later, in order to unblock the client with a call to
 * RedisModule_UnblockClient(). The arguments specify callback functions
 * and a timeout after which the client is unblocked.
 *
 * The callbacks are called in the following contexts:
 *
 *     reply_callback:  called after a successful RedisModule_UnblockClient()
 *                      call in order to reply to the client and unblock it.
 *
 *     reply_timeout:   called when the timeout is reached in order to send an
 *                      error to the client.
 *
 *     free_privdata:   called in order to free the private data that is passed
 *                      by RedisModule_UnblockClient() call.
 */
RedisModuleBlockedClient *RM_BlockClient(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_callback, RedisModuleCmdFunc timeout_callback, void (*free_privdata)(RedisModuleCtx *, void *), long long timeout_ms)
{
    client *c = ctx->client;
    int islua = c->flags & CLIENT_LUA;
    int ismulti = c->flags & CLIENT_MULTI;

    c->bpop.module_blocked_handle = zmalloc(sizeof(RedisModuleBlockedClient));
    RedisModuleBlockedClient *bc = c->bpop.module_blocked_handle;

    /* We need to handle the invalid operation of calling modules blocking
     * commands from Lua or MULTI. We actually create an already aborted
     * (client set to NULL) blocked client handle, and actually reply with
     * an error. */
    bc->client = (islua || ismulti) ? NULL : c;
    bc->module = ctx->module;
    bc->reply_callback = reply_callback;
    bc->timeout_callback = timeout_callback;
    bc->disconnect_callback = NULL; /* Set by RM_SetDisconnectCallback() */
    bc->free_privdata = free_privdata;
    bc->privdata = NULL;
    bc->reply_client = createClient(-1);
    bc->reply_client->flags |= CLIENT_MODULE;
    bc->dbid = c->db->id;
    c->bpop.timeout = timeout_ms ? (mstime() + timeout_ms) : 0;

    if (islua || ismulti)
    {
        c->bpop.module_blocked_handle = NULL;
        addReplyError(c, islua ? "Blocking module command called from Lua script" : "Blocking module command called from transaction");
    }
    else
    {
        blockClient(c, BLOCKED_MODULE);
    }
    return bc;
}

/* Unblock a client blocked by `RedisModule_BlockedClient`. This will trigger
 * the reply callbacks to be called in order to reply to the client.
 * The 'privdata' argument will be accessible by the reply callback, so
 * the caller of this function can pass any value that is needed in order to
 * actually reply to the client.
 *
 * A common usage for 'privdata' is a thread that computes something that
 * needs to be passed to the client, included but not limited some slow
 * to compute reply or some reply obtained via networking.
 *
 * Note: this function can be called from threads spawned by the module. */
int RM_UnblockClient(RedisModuleBlockedClient *bc, void *privdata)
{
    pthread_mutex_lock(&moduleUnblockedClientsMutex);
    bc->privdata = privdata;
    listAddNodeTail(moduleUnblockedClients, bc);
    if (write(server.module_blocked_pipe[1], "A", 1) != 1)
    {
        /* Ignore the error, this is best-effort. */
    }
    pthread_mutex_unlock(&moduleUnblockedClientsMutex);
    return REDISMODULE_OK;
}

/* Abort a blocked client blocking operation: the client will be unblocked
 * without firing any callback. */
int RM_AbortBlock(RedisModuleBlockedClient *bc)
{
    bc->reply_callback = NULL;
    bc->disconnect_callback = NULL;
    return RM_UnblockClient(bc, NULL);
}

/* Set a callback that will be called if a blocked client disconnects
 * before the module has a chance to call RedisModule_UnblockClient()
 *
 * Usually what you want to do there, is to cleanup your module state
 * so that you can call RedisModule_UnblockClient() safely, otherwise
 * the client will remain blocked forever if the timeout is large.
 *
 * Notes:
 *
 * 1. It is not safe to call Reply* family functions here, it is also
 *    useless since the client is gone.
 *
 * 2. This callback is not called if the client disconnects because of
 *    a timeout. In such a case, the client is unblocked automatically
 *    and the timeout callback is called.
 */
void RM_SetDisconnectCallback(RedisModuleBlockedClient *bc, RedisModuleDisconnectFunc callback)
{
    bc->disconnect_callback = callback;
}

/* This function will check the moduleUnblockedClients queue in order to
 * call the reply callback and really unblock the client.
 *
 * Clients end into this list because of calls to RM_UnblockClient(),
 * however it is possible that while the module was doing work for the
 * blocked client, it was terminated by Redis (for timeout or other reasons).
 * When this happens the RedisModuleBlockedClient structure in the queue
 * will have the 'client' field set to NULL. */
void moduleHandleBlockedClients(void)
{
    listNode *ln;
    RedisModuleBlockedClient *bc;

    pthread_mutex_lock(&moduleUnblockedClientsMutex);
    /* Here we unblock all the pending clients blocked in modules operations
     * so we can read every pending "awake byte" in the pipe. */
    char buf[1];
    while (read(server.module_blocked_pipe[0], buf, 1) == 1)
        ;
    while (listLength(moduleUnblockedClients))
    {
        ln = listFirst(moduleUnblockedClients);
        bc = ln->value;
        client *c = bc->client;
        listDelNode(moduleUnblockedClients, ln);
        pthread_mutex_unlock(&moduleUnblockedClientsMutex);

        /* Release the lock during the loop, as long as we don't
         * touch the shared list. */

        /* Call the reply callback if the client is valid and we have
         * any callback. */
        if (c && bc->reply_callback)
        {
            RedisModuleCtx ctx = REDISMODULE_CTX_INIT;
            ctx.flags |= REDISMODULE_CTX_BLOCKED_REPLY;
            ctx.blocked_privdata = bc->privdata;
            ctx.module = bc->module;
            ctx.client = bc->client;
            ctx.blocked_client = bc;
            bc->reply_callback(&ctx, (void **)c->argv, c->argc);
            moduleHandlePropagationAfterCommandCallback(&ctx);
            moduleFreeContext(&ctx);
        }

        /* Free privdata if any. */
        if (bc->privdata && bc->free_privdata)
        {
            RedisModuleCtx ctx = REDISMODULE_CTX_INIT;
            if (c == NULL)
                ctx.flags |= REDISMODULE_CTX_BLOCKED_DISCONNECTED;
            ctx.blocked_privdata = bc->privdata;
            ctx.module = bc->module;
            ctx.client = bc->client;
            bc->free_privdata(&ctx, bc->privdata);
            moduleFreeContext(&ctx);
        }

        /* It is possible that this blocked client object accumulated
         * replies to send to the client in a thread safe context.
         * We need to glue such replies to the client output buffer and
         * free the temporary client we just used for the replies. */
        if (c)
            AddReplyFromClient(c, bc->reply_client);
        freeClient(bc->reply_client);

        if (c != NULL)
        {
            /* Before unblocking the client, set the disconnect callback
             * to NULL, because if we reached this point, the client was
             * properly unblocked by the module. */
            bc->disconnect_callback = NULL;
            unblockClient(c);
            /* Put the client in the list of clients that need to write
             * if there are pending replies here. This is needed since
             * during a non blocking command the client may receive output. */
            if (clientHasPendingReplies(c) &&
                !(c->flags & CLIENT_PENDING_WRITE))
            {
                c->flags |= CLIENT_PENDING_WRITE;
                listAddNodeHead(server.clients_pending_write, c);
            }
        }

        /* Free 'bc' only after unblocking the client, since it is
         * referenced in the client blocking context, and must be valid
         * when calling unblockClient(). */
        zfree(bc);

        /* Lock again before to iterate the loop. */
        pthread_mutex_lock(&moduleUnblockedClientsMutex);
    }
    pthread_mutex_unlock(&moduleUnblockedClientsMutex);
}

/* Called when our client timed out. After this function unblockClient()
 * is called, and it will invalidate the blocked client. So this function
 * does not need to do any cleanup. Eventually the module will call the
 * API to unblock the client and the memory will be released. */
void moduleBlockedClientTimedOut(client *c)
{
    RedisModuleBlockedClient *bc = c->bpop.module_blocked_handle;
    RedisModuleCtx ctx = REDISMODULE_CTX_INIT;
    ctx.flags |= REDISMODULE_CTX_BLOCKED_TIMEOUT;
    ctx.module = bc->module;
    ctx.client = bc->client;
    ctx.blocked_client = bc;
    bc->timeout_callback(&ctx, (void **)c->argv, c->argc);
    moduleFreeContext(&ctx);
    /* For timeout events, we do not want to call the disconnect callback,
     * because the blocked client will be automatically disconnected in
     * this case, and the user can still hook using the timeout callback. */
    bc->disconnect_callback = NULL;
}

/* Return non-zero if a module command was called in order to fill the
 * reply for a blocked client. */
int RM_IsBlockedReplyRequest(RedisModuleCtx *ctx)
{
    return (ctx->flags & REDISMODULE_CTX_BLOCKED_REPLY) != 0;
}

/* Return non-zero if a module command was called in order to fill the
 * reply for a blocked client that timed out. */
int RM_IsBlockedTimeoutRequest(RedisModuleCtx *ctx)
{
    return (ctx->flags & REDISMODULE_CTX_BLOCKED_TIMEOUT) != 0;
}

/* Get the private data set by RedisModule_UnblockClient() */
void *RM_GetBlockedClientPrivateData(RedisModuleCtx *ctx)
{
    return ctx->blocked_privdata;
}

/* Get the blocked client associated with a given context.
 * This is useful in the reply and timeout callbacks of blocked clients,
 * before sometimes the module has the blocked client handle references
 * around, and wants to cleanup it. */
RedisModuleBlockedClient *RM_GetBlockedClientHandle(RedisModuleCtx *ctx)
{
    return ctx->blocked_client;
}

/* Return true if when the free callback of a blocked client is called,
 * the reason for the client to be unblocked is that it disconnected
 * while it was blocked. */
int RM_BlockedClientDisconnected(RedisModuleCtx *ctx)
{
    return (ctx->flags & REDISMODULE_CTX_BLOCKED_DISCONNECTED) != 0;
}

/* --------------------------------------------------------------------------
 * Thread Safe Contexts
 * -------------------------------------------------------------------------- */

/* Return a context which can be used inside threads to make Redis context
 * calls with certain modules APIs. If 'bc' is not NULL then the module will
 * be bound to a blocked client, and it will be possible to use the
 * `RedisModule_Reply*` family of functions to accumulate a reply for when the
 * client will be unblocked. Otherwise the thread safe context will be
 * detached by a specific client.
 *
 * To call non-reply APIs, the thread safe context must be prepared with:
 *
 *     RedisModule_ThreadSafeContextLock(ctx);
 *     ... make your call here ...
 *     RedisModule_ThreadSafeContextUnlock(ctx);
 *
 * This is not needed when using `RedisModule_Reply*` functions, assuming
 * that a blocked client was used when the context was created, otherwise
 * no RedisModule_Reply* call should be made at all.
 *
 * TODO: thread safe contexts do not inherit the blocked client
 * selected database. */
RedisModuleCtx *RM_GetThreadSafeContext(RedisModuleBlockedClient *bc)
{
    RedisModuleCtx *ctx = zmalloc(sizeof(*ctx));
    RedisModuleCtx empty = REDISMODULE_CTX_INIT;
    memcpy(ctx, &empty, sizeof(empty));
    if (bc)
    {
        ctx->blocked_client = bc;
        ctx->module = bc->module;
    }
    ctx->flags |= REDISMODULE_CTX_THREAD_SAFE;
    /* Even when the context is associated with a blocked client, we can't
     * access it safely from another thread, so we create a fake client here
     * in order to keep things like the currently selected database and similar
     * things. */
    ctx->client = createClient(-1);
    if (bc)
    {
        selectDb(ctx->client, bc->dbid);
        if (bc->client)
            ctx->client->id = bc->client->id;
    }
    return ctx;
}

/* Release a thread safe context. */
void RM_FreeThreadSafeContext(RedisModuleCtx *ctx)
{
    moduleFreeContext(ctx);
    zfree(ctx);
}

/* Acquire the server lock before executing a thread safe API call.
 * This is not needed for `RedisModule_Reply*` calls when there is
 * a blocked client connected to the thread safe context. */
void RM_ThreadSafeContextLock(RedisModuleCtx *ctx)
{
    UNUSED(ctx);
    moduleAcquireGIL();
}

/* Release the server lock after a thread safe API call was executed. */
void RM_ThreadSafeContextUnlock(RedisModuleCtx *ctx)
{
    UNUSED(ctx);
    moduleReleaseGIL();
}

void moduleAcquireGIL(void)
{
    pthread_mutex_lock(&moduleGIL);
}

void moduleReleaseGIL(void)
{
    pthread_mutex_unlock(&moduleGIL);
}

/* --------------------------------------------------------------------------
 * Module Keyspace Notifications API
 * -------------------------------------------------------------------------- */

/* Subscribe to keyspace notifications. This is a low-level version of the
 * keyspace-notifications API. A module can register callbacks to be notified
 * when keyspce events occur.
 *
 * Notification events are filtered by their type (string events, set events,
 * etc), and the subscriber callback receives only events that match a specific
 * mask of event types.
 *
 * When subscribing to notifications with RedisModule_SubscribeToKeyspaceEvents
 * the module must provide an event type-mask, denoting the events the subscriber
 * is interested in. This can be an ORed mask of any of the following flags:
 *
 *  - REDISMODULE_NOTIFY_GENERIC: Generic commands like DEL, EXPIRE, RENAME
 *  - REDISMODULE_NOTIFY_STRING: String events
 *  - REDISMODULE_NOTIFY_LIST: List events
 *  - REDISMODULE_NOTIFY_SET: Set events
 *  - REDISMODULE_NOTIFY_HASH: Hash events
 *  - REDISMODULE_NOTIFY_ZSET: Sorted Set events
 *  - REDISMODULE_NOTIFY_EXPIRED: Expiration events
 *  - REDISMODULE_NOTIFY_EVICTED: Eviction events
 *  - REDISMODULE_NOTIFY_STREAM: Stream events
 *  - REDISMODULE_NOTIFY_ALL: All events
 *
 * We do not distinguish between key events and keyspace events, and it is up
 * to the module to filter the actions taken based on the key.
 *
 * The subscriber signature is:
 *
 *   int (*RedisModuleNotificationFunc) (RedisModuleCtx *ctx, int type,
 *                                       const char *event,
 *                                       RedisModuleString *key);
 *
 * `type` is the event type bit, that must match the mask given at registration
 * time. The event string is the actual command being executed, and key is the
 * relevant Redis key.
 *
 * Notification callback gets executed with a redis context that can not be
 * used to send anything to the client, and has the db number where the event
 * occurred as its selected db number.
 *
 * Notice that it is not necessary to enable notifications in redis.conf for
 * module notifications to work.
 *
 * Warning: the notification callbacks are performed in a synchronous manner,
 * so notification callbacks must to be fast, or they would slow Redis down.
 * If you need to take long actions, use threads to offload them.
 *
 * See https://redis.io/topics/notifications for more information.
 */
int RM_SubscribeToKeyspaceEvents(RedisModuleCtx *ctx, int types, RedisModuleNotificationFunc callback)
{
    RedisModuleKeyspaceSubscriber *sub = zmalloc(sizeof(*sub));
    sub->module = ctx->module;
    sub->event_mask = types;
    sub->notify_callback = callback;
    sub->active = 0;

    listAddNodeTail(moduleKeyspaceSubscribers, sub);
    return REDISMODULE_OK;
}

/* Dispatcher for keyspace notifications to module subscriber functions.
 * This gets called  only if at least one module requested to be notified on
 * keyspace notifications */
void moduleNotifyKeyspaceEvent(int type, const char *event, robj *key, int dbid)
{
    /* Don't do anything if there aren't any subscribers */
    if (listLength(moduleKeyspaceSubscribers) == 0)
        return;

    listIter li;
    listNode *ln;
    listRewind(moduleKeyspaceSubscribers, &li);

    /* Remove irrelevant flags from the type mask */
    type &= ~(NOTIFY_KEYEVENT | NOTIFY_KEYSPACE);

    while ((ln = listNext(&li)))
    {
        RedisModuleKeyspaceSubscriber *sub = ln->value;
        /* Only notify subscribers on events matching they registration,
         * and avoid subscribers triggering themselves */
        if ((sub->event_mask & type) && sub->active == 0)
        {
            RedisModuleCtx ctx = REDISMODULE_CTX_INIT;
            ctx.module = sub->module;
            ctx.client = moduleFreeContextReusedClient;
            selectDb(ctx.client, dbid);

            /* mark the handler as active to avoid reentrant loops.
             * If the subscriber performs an action triggering itself,
             * it will not be notified about it. */
            sub->active = 1;
            sub->notify_callback(&ctx, type, event, key);
            sub->active = 0;
            moduleFreeContext(&ctx);
        }
    }
}

/* Unsubscribe any notification subscribers this module has upon unloading */
void moduleUnsubscribeNotifications(RedisModule *module)
{
    listIter li;
    listNode *ln;
    listRewind(moduleKeyspaceSubscribers, &li);
    while ((ln = listNext(&li)))
    {
        RedisModuleKeyspaceSubscriber *sub = ln->value;
        if (sub->module == module)
        {
            listDelNode(moduleKeyspaceSubscribers, ln);
            zfree(sub);
        }
    }
}

/* --------------------------------------------------------------------------
 * Modules Cluster API
 * -------------------------------------------------------------------------- */

/* The Cluster message callback function pointer type. */
typedef void (*RedisModuleClusterMessageReceiver)(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len);

/* This structure identifies a registered caller: it must match a given module
 * ID, for a given message type. The callback function is just the function
 * that was registered as receiver. */
typedef struct moduleClusterReceiver
{
    uint64_t module_id;
    RedisModuleClusterMessageReceiver callback;
    struct RedisModule *module;
    struct moduleClusterReceiver *next;
} moduleClusterReceiver;

typedef struct moduleClusterNodeInfo
{
    int flags;
    char ip[NET_IP_STR_LEN];
    int port;
    char master_id[40]; /* Only if flags & REDISMODULE_NODE_MASTER is true. */
} mdouleClusterNodeInfo;

/* We have an array of message types: each bucket is a linked list of
 * configured receivers. */
static moduleClusterReceiver *clusterReceivers[UINT8_MAX];

/* Dispatch the message to the right module receiver. */
void moduleCallClusterReceivers(const char *sender_id, uint64_t module_id, uint8_t type, const unsigned char *payload, uint32_t len)
{
    moduleClusterReceiver *r = clusterReceivers[type];
    while (r)
    {
        if (r->module_id == module_id)
        {
            RedisModuleCtx ctx = REDISMODULE_CTX_INIT;
            ctx.module = r->module;
            ctx.client = moduleFreeContextReusedClient;
            selectDb(ctx.client, 0);
            r->callback(&ctx, sender_id, type, payload, len);
            moduleFreeContext(&ctx);
            return;
        }
        r = r->next;
    }
}

/* Register a callback receiver for cluster messages of type 'type'. If there
 * was already a registered callback, this will replace the callback function
 * with the one provided, otherwise if the callback is set to NULL and there
 * is already a callback for this function, the callback is unregistered
 * (so this API call is also used in order to delete the receiver). */
void RM_RegisterClusterMessageReceiver(RedisModuleCtx *ctx, uint8_t type, RedisModuleClusterMessageReceiver callback)
{
    if (!server.cluster_enabled)
        return;

    uint64_t module_id = moduleTypeEncodeId(ctx->module->name, 0);
    moduleClusterReceiver *r = clusterReceivers[type], *prev = NULL;
    while (r)
    {
        if (r->module_id == module_id)
        {
            /* Found! Set or delete. */
            if (callback)
            {
                r->callback = callback;
            }
            else
            {
                /* Delete the receiver entry if the user is setting
                 * it to NULL. Just unlink the receiver node from the
                 * linked list. */
                if (prev)
                    prev->next = r->next;
                else
                    clusterReceivers[type]->next = r->next;
                zfree(r);
            }
            return;
        }
        prev = r;
        r = r->next;
    }

    /* Not found, let's add it. */
    if (callback)
    {
        r = zmalloc(sizeof(*r));
        r->module_id = module_id;
        r->module = ctx->module;
        r->callback = callback;
        r->next = clusterReceivers[type];
        clusterReceivers[type] = r;
    }
}

/* Send a message to all the nodes in the cluster if `target` is NULL, otherwise
 * at the specified target, which is a REDISMODULE_NODE_ID_LEN bytes node ID, as
 * returned by the receiver callback or by the nodes iteration functions.
 *
 * The function returns REDISMODULE_OK if the message was successfully sent,
 * otherwise if the node is not connected or such node ID does not map to any
 * known cluster node, REDISMODULE_ERR is returned. */
int RM_SendClusterMessage(RedisModuleCtx *ctx, char *target_id, uint8_t type, unsigned char *msg, uint32_t len)
{
    if (!server.cluster_enabled)
        return REDISMODULE_ERR;
    uint64_t module_id = moduleTypeEncodeId(ctx->module->name, 0);
    if (clusterSendModuleMessageToTarget(target_id, module_id, type, msg, len) == C_OK)
        return REDISMODULE_OK;
    else
        return REDISMODULE_ERR;
}

/* Return an array of string pointers, each string pointer points to a cluster
 * node ID of exactly REDISMODULE_NODE_ID_SIZE bytes (without any null term).
 * The number of returned node IDs is stored into `*numnodes`.
 * However if this function is called by a module not running an a Redis
 * instance with Redis Cluster enabled, NULL is returned instead.
 *
 * The IDs returned can be used with RedisModule_GetClusterNodeInfo() in order
 * to get more information about single nodes.
 *
 * The array returned by this function must be freed using the function
 * RedisModule_FreeClusterNodesList().
 *
 * Example:
 *
 *     size_t count, j;
 *     char **ids = RedisModule_GetClusterNodesList(ctx,&count);
 *     for (j = 0; j < count; j++) {
 *         RedisModule_Log("notice","Node %.*s",
 *             REDISMODULE_NODE_ID_LEN,ids[j]);
 *     }
 *     RedisModule_FreeClusterNodesList(ids);
 */
char **RM_GetClusterNodesList(RedisModuleCtx *ctx, size_t *numnodes)
{
    UNUSED(ctx);

    if (!server.cluster_enabled)
        return NULL;
    size_t count = dictSize(server.cluster->nodes);
    char **ids = zmalloc((count + 1) * REDISMODULE_NODE_ID_LEN);
    dictIterator *di = dictGetIterator(server.cluster->nodes);
    dictEntry *de;
    int j = 0;
    while ((de = dictNext(di)) != NULL)
    {
        clusterNode *node = dictGetVal(de);
        if (node->flags & (CLUSTER_NODE_NOADDR | CLUSTER_NODE_HANDSHAKE))
            continue;
        ids[j] = zmalloc(REDISMODULE_NODE_ID_LEN);
        memcpy(ids[j], node->name, REDISMODULE_NODE_ID_LEN);
        j++;
    }
    *numnodes = j;
    ids[j] = NULL; /* Null term so that FreeClusterNodesList does not need
                    * to also get the count argument. */
    dictReleaseIterator(di);
    return ids;
}

/* Free the node list obtained with RedisModule_GetClusterNodesList. */
void RM_FreeClusterNodesList(char **ids)
{
    if (ids == NULL)
        return;
    for (int j = 0; ids[j]; j++)
        zfree(ids[j]);
    zfree(ids);
}

/* Return this node ID (REDISMODULE_CLUSTER_ID_LEN bytes) or NULL if the cluster
 * is disabled. */
const char *RM_GetMyClusterID(void)
{
    if (!server.cluster_enabled)
        return NULL;
    return server.cluster->myself->name;
}

/* Return the number of nodes in the cluster, regardless of their state
 * (handshake, noaddress, ...) so that the number of active nodes may actually
 * be smaller, but not greater than this number. If the instance is not in
 * cluster mode, zero is returned. */
size_t RM_GetClusterSize(void)
{
    if (!server.cluster_enabled)
        return 0;
    return dictSize(server.cluster->nodes);
}

/* Populate the specified info for the node having as ID the specified 'id',
 * then returns REDISMODULE_OK. Otherwise if the node ID does not exist from
 * the POV of this local node, REDISMODULE_ERR is returned.
 *
 * The arguments ip, master_id, port and flags can be NULL in case we don't
 * need to populate back certain info. If an ip and master_id (only populated
 * if the instance is a slave) are specified, they point to buffers holding
 * at least REDISMODULE_NODE_ID_LEN bytes. The strings written back as ip
 * and master_id are not null terminated.
 *
 * The list of flags reported is the following:
 *
 * * REDISMODULE_NODE_MYSELF        This node
 * * REDISMODULE_NODE_MASTER        The node is a master
 * * REDISMODULE_NODE_SLAVE         The node is a replica
 * * REDISMODULE_NODE_PFAIL         We see the node as failing
 * * REDISMODULE_NODE_FAIL          The cluster agrees the node is failing
 * * REDISMODULE_NODE_NOFAILOVER    The slave is configured to never failover
 */

clusterNode *clusterLookupNode(const char *name); /* We need access to internals */

int RM_GetClusterNodeInfo(RedisModuleCtx *ctx, const char *id, char *ip, char *master_id, int *port, int *flags)
{
    UNUSED(ctx);

    clusterNode *node = clusterLookupNode(id);
    if (node == NULL ||
        node->flags & (CLUSTER_NODE_NOADDR | CLUSTER_NODE_HANDSHAKE))
    {
        return REDISMODULE_ERR;
    }

    if (ip)
        strncpy(ip, node->ip, NET_IP_STR_LEN);

    if (master_id)
    {
        /* If the information is not available, the function will set the
         * field to zero bytes, so that when the field can't be populated the
         * function kinda remains predictable. */
        if (node->flags & CLUSTER_NODE_MASTER && node->slaveof)
            memcpy(master_id, node->slaveof->name, REDISMODULE_NODE_ID_LEN);
        else
            memset(master_id, 0, REDISMODULE_NODE_ID_LEN);
    }
    if (port)
        *port = node->port;

    /* As usually we have to remap flags for modules, in order to ensure
     * we can provide binary compatibility. */
    if (flags)
    {
        *flags = 0;
        if (node->flags & CLUSTER_NODE_MYSELF)
            *flags |= REDISMODULE_NODE_MYSELF;
        if (node->flags & CLUSTER_NODE_MASTER)
            *flags |= REDISMODULE_NODE_MASTER;
        if (node->flags & CLUSTER_NODE_SLAVE)
            *flags |= REDISMODULE_NODE_SLAVE;
        if (node->flags & CLUSTER_NODE_PFAIL)
            *flags |= REDISMODULE_NODE_PFAIL;
        if (node->flags & CLUSTER_NODE_FAIL)
            *flags |= REDISMODULE_NODE_FAIL;
        if (node->flags & CLUSTER_NODE_NOFAILOVER)
            *flags |= REDISMODULE_NODE_NOFAILOVER;
    }
    return REDISMODULE_OK;
}

/* Set Redis Cluster flags in order to change the normal behavior of
 * Redis Cluster, especially with the goal of disabling certain functions.
 * This is useful for modules that use the Cluster API in order to create
 * a different distributed system, but still want to use the Redis Cluster
 * message bus. Flags that can be set:
 *
 *  CLUSTER_MODULE_FLAG_NO_FAILOVER
 *  CLUSTER_MODULE_FLAG_NO_REDIRECTION
 *
 * With the following effects:
 *
 *  NO_FAILOVER: prevent Redis Cluster slaves to failover a failing master.
 *               Also disables the replica migration feature.
 *
 *  NO_REDIRECTION: Every node will accept any key, without trying to perform
 *                  partitioning according to the user Redis Cluster algorithm.
 *                  Slots informations will still be propagated across the
 *                  cluster, but without effects. */
void RM_SetClusterFlags(RedisModuleCtx *ctx, uint64_t flags)
{
    UNUSED(ctx);
    if (flags & REDISMODULE_CLUSTER_FLAG_NO_FAILOVER)
        server.cluster_module_flags |= CLUSTER_MODULE_FLAG_NO_FAILOVER;
    if (flags & REDISMODULE_CLUSTER_FLAG_NO_REDIRECTION)
        server.cluster_module_flags |= CLUSTER_MODULE_FLAG_NO_REDIRECTION;
}

/* --------------------------------------------------------------------------
 * Modules Timers API
 *
 * Module timers are an high precision "green timers" abstraction where
 * every module can register even millions of timers without problems, even if
 * the actual event loop will just have a single timer that is used to awake the
 * module timers subsystem in order to process the next event.
 *
 * All the timers are stored into a radix tree, ordered by expire time, when
 * the main Redis event loop timer callback is called, we try to process all
 * the timers already expired one after the other. Then we re-enter the event
 * loop registering a timer that will expire when the next to process module
 * timer will expire.
 *
 * Every time the list of active timers drops to zero, we unregister the
 * main event loop timer, so that there is no overhead when such feature is
 * not used.
 * -------------------------------------------------------------------------- */

static rax *Timers;     /* The radix tree of all the timers sorted by expire. */
long long aeTimer = -1; /* Main event loop (ae.c) timer identifier. */

typedef void (*RedisModuleTimerProc)(RedisModuleCtx *ctx, void *data);

/* The timer descriptor, stored as value in the radix tree. */
typedef struct RedisModuleTimer
{
    RedisModule *module;           /* Module reference. */
    RedisModuleTimerProc callback; /* The callback to invoke on expire. */
    void *data;                    /* Private data for the callback. */
    int dbid;                      /* Database number selected by the original client. */
} RedisModuleTimer;

/* This is the timer handler that is called by the main event loop. We schedule
 * this timer to be called when the nearest of our module timers will expire. */
int moduleTimerHandler(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    /* To start let's try to fire all the timers already expired. */
    raxIterator ri;
    raxStart(&ri, Timers);
    uint64_t now = ustime();
    long long next_period = 0;
    while (1)
    {
        raxSeek(&ri, "^", NULL, 0);
        if (!raxNext(&ri))
            break;
        uint64_t expiretime;
        memcpy(&expiretime, ri.key, sizeof(expiretime));
        expiretime = ntohu64(expiretime);
        if (now >= expiretime)
        {
            RedisModuleTimer *timer = ri.data;
            RedisModuleCtx ctx = REDISMODULE_CTX_INIT;

            ctx.module = timer->module;
            ctx.client = moduleFreeContextReusedClient;
            selectDb(ctx.client, timer->dbid);
            timer->callback(&ctx, timer->data);
            moduleFreeContext(&ctx);
            raxRemove(Timers, (unsigned char *)ri.key, ri.key_len, NULL);
            zfree(timer);
        }
        else
        {
            next_period = (expiretime - now) / 1000; /* Scale to milliseconds. */
            break;
        }
    }
    raxStop(&ri);

    /* Reschedule the next timer or cancel it. */
    if (next_period <= 0)
        next_period = 1;
    return (raxSize(Timers) > 0) ? next_period : AE_NOMORE;
}

/* Create a new timer that will fire after `period` milliseconds, and will call
 * the specified function using `data` as argument. The returned timer ID can be
 * used to get information from the timer or to stop it before it fires. */
RedisModuleTimerID RM_CreateTimer(RedisModuleCtx *ctx, mstime_t period, RedisModuleTimerProc callback, void *data)
{
    RedisModuleTimer *timer = zmalloc(sizeof(*timer));
    timer->module = ctx->module;
    timer->callback = callback;
    timer->data = data;
    timer->dbid = ctx->client->db->id;
    uint64_t expiretime = ustime() + period * 1000;
    uint64_t key;

    while (1)
    {
        key = htonu64(expiretime);
        if (raxFind(Timers, (unsigned char *)&key, sizeof(key)) == raxNotFound)
        {
            raxInsert(Timers, (unsigned char *)&key, sizeof(key), timer, NULL);
            break;
        }
        else
        {
            expiretime++;
        }
    }

    /* We need to install the main event loop timer if it's not already
     * installed, or we may need to refresh its period if we just installed
     * a timer that will expire sooner than any other else. */
    if (aeTimer != -1)
    {
        raxIterator ri;
        raxStart(&ri, Timers);
        raxSeek(&ri, "^", NULL, 0);
        raxNext(&ri);
        if (memcmp(ri.key, &key, sizeof(key)) == 0)
        {
            /* This is the first key, we need to re-install the timer according
             * to the just added event. */
            aeDeleteTimeEvent(server.el, aeTimer);
            aeTimer = -1;
        }
        raxStop(&ri);
    }

    /* If we have no main timer (the old one was invalidated, or this is the
     * first module timer we have), install one. */
    if (aeTimer == -1)
        aeTimer = aeCreateTimeEvent(server.el, period, moduleTimerHandler, NULL, NULL);

    return key;
}

/* Stop a timer, returns REDISMODULE_OK if the timer was found, belonged to the
 * calling module, and was stopped, otherwise REDISMODULE_ERR is returned.
 * If not NULL, the data pointer is set to the value of the data argument when
 * the timer was created. */
int RM_StopTimer(RedisModuleCtx *ctx, RedisModuleTimerID id, void **data)
{
    RedisModuleTimer *timer = raxFind(Timers, (unsigned char *)&id, sizeof(id));
    if (timer == raxNotFound || timer->module != ctx->module)
        return REDISMODULE_ERR;
    if (data)
        *data = timer->data;
    raxRemove(Timers, (unsigned char *)&id, sizeof(id), NULL);
    zfree(timer);
    return REDISMODULE_OK;
}

/* Obtain information about a timer: its remaining time before firing
 * (in milliseconds), and the private data pointer associated with the timer.
 * If the timer specified does not exist or belongs to a different module
 * no information is returned and the function returns REDISMODULE_ERR, otherwise
 * REDISMODULE_OK is returned. The arguments remaining or data can be NULL if
 * the caller does not need certain information. */
int RM_GetTimerInfo(RedisModuleCtx *ctx, RedisModuleTimerID id, uint64_t *remaining, void **data)
{
    RedisModuleTimer *timer = raxFind(Timers, (unsigned char *)&id, sizeof(id));
    if (timer == raxNotFound || timer->module != ctx->module)
        return REDISMODULE_ERR;
    if (remaining)
    {
        int64_t rem = ntohu64(id) - ustime();
        if (rem < 0)
            rem = 0;
        *remaining = rem / 1000; /* Scale to milliseconds. */
    }
    if (data)
        *data = timer->data;
    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Modules Dictionary API
 *
 * Implements a sorted dictionary (actually backed by a radix tree) with
 * the usual get / set / del / num-items API, together with an iterator
 * capable of going back and forth.
 * -------------------------------------------------------------------------- */

/* Create a new dictionary. The 'ctx' pointer can be the current module context
 * or NULL, depending on what you want. Please follow the following rules:
 *
 * 1. Use a NULL context if you plan to retain a reference to this dictionary
 *    that will survive the time of the module callback where you created it.
 * 2. Use a NULL context if no context is available at the time you are creating
 *    the dictionary (of course...).
 * 3. However use the current callback context as 'ctx' argument if the
 *    dictionary time to live is just limited to the callback scope. In this
 *    case, if enabled, you can enjoy the automatic memory management that will
 *    reclaim the dictionary memory, as well as the strings returned by the
 *    Next / Prev dictionary iterator calls.
 */
RedisModuleDict *RM_CreateDict(RedisModuleCtx *ctx)
{
    struct RedisModuleDict *d = zmalloc(sizeof(*d));
    d->rax = raxNew();
    if (ctx != NULL)
        autoMemoryAdd(ctx, REDISMODULE_AM_DICT, d);
    return d;
}

/* Free a dictionary created with RM_CreateDict(). You need to pass the
 * context pointer 'ctx' only if the dictionary was created using the
 * context instead of passing NULL. */
void RM_FreeDict(RedisModuleCtx *ctx, RedisModuleDict *d)
{
    if (ctx != NULL)
        autoMemoryFreed(ctx, REDISMODULE_AM_DICT, d);
    raxFree(d->rax);
    zfree(d);
}

/* Return the size of the dictionary (number of keys). */
uint64_t RM_DictSize(RedisModuleDict *d)
{
    return raxSize(d->rax);
}

/* Store the specified key into the dictionary, setting its value to the
 * pointer 'ptr'. If the key was added with success, since it did not
 * already exist, REDISMODULE_OK is returned. Otherwise if the key already
 * exists the function returns REDISMODULE_ERR. */
int RM_DictSetC(RedisModuleDict *d, void *key, size_t keylen, void *ptr)
{
    int retval = raxTryInsert(d->rax, key, keylen, ptr, NULL);
    return (retval == 1) ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Like RedisModule_DictSetC() but will replace the key with the new
 * value if the key already exists. */
int RM_DictReplaceC(RedisModuleDict *d, void *key, size_t keylen, void *ptr)
{
    int retval = raxInsert(d->rax, key, keylen, ptr, NULL);
    return (retval == 1) ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Like RedisModule_DictSetC() but takes the key as a RedisModuleString. */
int RM_DictSet(RedisModuleDict *d, RedisModuleString *key, void *ptr)
{
    return RM_DictSetC(d, key->ptr, sdslen(key->ptr), ptr);
}

/* Like RedisModule_DictReplaceC() but takes the key as a RedisModuleString. */
int RM_DictReplace(RedisModuleDict *d, RedisModuleString *key, void *ptr)
{
    return RM_DictReplaceC(d, key->ptr, sdslen(key->ptr), ptr);
}

/* Return the value stored at the specified key. The function returns NULL
 * both in the case the key does not exist, or if you actually stored
 * NULL at key. So, optionally, if the 'nokey' pointer is not NULL, it will
 * be set by reference to 1 if the key does not exist, or to 0 if the key
 * exists. */
void *RM_DictGetC(RedisModuleDict *d, void *key, size_t keylen, int *nokey)
{
    void *res = raxFind(d->rax, key, keylen);
    if (nokey)
        *nokey = (res == raxNotFound);
    return (res == raxNotFound) ? NULL : res;
}

/* Like RedisModule_DictGetC() but takes the key as a RedisModuleString. */
void *RM_DictGet(RedisModuleDict *d, RedisModuleString *key, int *nokey)
{
    return RM_DictGetC(d, key->ptr, sdslen(key->ptr), nokey);
}

/* Remove the specified key from the dictionary, returning REDISMODULE_OK if
 * the key was found and delted, or REDISMODULE_ERR if instead there was
 * no such key in the dictionary. When the operation is successful, if
 * 'oldval' is not NULL, then '*oldval' is set to the value stored at the
 * key before it was deleted. Using this feature it is possible to get
 * a pointer to the value (for instance in order to release it), without
 * having to call RedisModule_DictGet() before deleting the key. */
int RM_DictDelC(RedisModuleDict *d, void *key, size_t keylen, void *oldval)
{
    int retval = raxRemove(d->rax, key, keylen, oldval);
    return retval ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Like RedisModule_DictDelC() but gets the key as a RedisModuleString. */
int RM_DictDel(RedisModuleDict *d, RedisModuleString *key, void *oldval)
{
    return RM_DictDelC(d, key->ptr, sdslen(key->ptr), oldval);
}

/* Return an interator, setup in order to start iterating from the specified
 * key by applying the operator 'op', which is just a string specifying the
 * comparison operator to use in order to seek the first element. The
 * operators avalable are:
 *
 * "^"   -- Seek the first (lexicographically smaller) key.
 * "$"   -- Seek the last  (lexicographically biffer) key.
 * ">"   -- Seek the first element greter than the specified key.
 * ">="  -- Seek the first element greater or equal than the specified key.
 * "<"   -- Seek the first element smaller than the specified key.
 * "<="  -- Seek the first element smaller or equal than the specified key.
 * "=="  -- Seek the first element matching exactly the specified key.
 *
 * Note that for "^" and "$" the passed key is not used, and the user may
 * just pass NULL with a length of 0.
 *
 * If the element to start the iteration cannot be seeked based on the
 * key and operator passed, RedisModule_DictNext() / Prev() will just return
 * REDISMODULE_ERR at the first call, otherwise they'll produce elements.
 */
RedisModuleDictIter *RM_DictIteratorStartC(RedisModuleDict *d, const char *op, void *key, size_t keylen)
{
    RedisModuleDictIter *di = zmalloc(sizeof(*di));
    di->dict = d;
    raxStart(&di->ri, d->rax);
    raxSeek(&di->ri, op, key, keylen);
    return di;
}

/* Exactly like RedisModule_DictIteratorStartC, but the key is passed as a
 * RedisModuleString. */
RedisModuleDictIter *RM_DictIteratorStart(RedisModuleDict *d, const char *op, RedisModuleString *key)
{
    return RM_DictIteratorStartC(d, op, key->ptr, sdslen(key->ptr));
}

/* Release the iterator created with RedisModule_DictIteratorStart(). This call
 * is mandatory otherwise a memory leak is introduced in the module. */
void RM_DictIteratorStop(RedisModuleDictIter *di)
{
    raxStop(&di->ri);
    zfree(di);
}

/* After its creation with RedisModule_DictIteratorStart(), it is possible to
 * change the currently selected element of the iterator by using this
 * API call. The result based on the operator and key is exactly like
 * the function RedisModule_DictIteratorStart(), however in this case the
 * return value is just REDISMODULE_OK in case the seeked element was found,
 * or REDISMODULE_ERR in case it was not possible to seek the specified
 * element. It is possible to reseek an iterator as many times as you want. */
int RM_DictIteratorReseekC(RedisModuleDictIter *di, const char *op, void *key, size_t keylen)
{
    return raxSeek(&di->ri, op, key, keylen);
}

/* Like RedisModule_DictIteratorReseekC() but takes the key as as a
 * RedisModuleString. */
int RM_DictIteratorReseek(RedisModuleDictIter *di, const char *op, RedisModuleString *key)
{
    return RM_DictIteratorReseekC(di, op, key->ptr, sdslen(key->ptr));
}

/* Return the current item of the dictionary iterator 'di' and steps to the
 * next element. If the iterator already yield the last element and there
 * are no other elements to return, NULL is returned, otherwise a pointer
 * to a string representing the key is provided, and the '*keylen' length
 * is set by reference (if keylen is not NULL). The '*dataptr', if not NULL
 * is set to the value of the pointer stored at the returned key as auxiliary
 * data (as set by the RedisModule_DictSet API).
 *
 * Usage example:
 *
 *      ... create the iterator here ...
 *      char *key;
 *      void *data;
 *      while((key = RedisModule_DictNextC(iter,&keylen,&data)) != NULL) {
 *          printf("%.*s %p\n", (int)keylen, key, data);
 *      }
 *
 * The returned pointer is of type void because sometimes it makes sense
 * to cast it to a char* sometimes to an unsigned char* depending on the
 * fact it contains or not binary data, so this API ends being more
 * comfortable to use.
 *
 * The validity of the returned pointer is until the next call to the
 * next/prev iterator step. Also the pointer is no longer valid once the
 * iterator is released. */
void *RM_DictNextC(RedisModuleDictIter *di, size_t *keylen, void **dataptr)
{
    if (!raxNext(&di->ri))
        return NULL;
    if (keylen)
        *keylen = di->ri.key_len;
    if (dataptr)
        *dataptr = di->ri.data;
    return di->ri.key;
}

/* This function is exactly like RedisModule_DictNext() but after returning
 * the currently selected element in the iterator, it selects the previous
 * element (laxicographically smaller) instead of the next one. */
void *RM_DictPrevC(RedisModuleDictIter *di, size_t *keylen, void **dataptr)
{
    if (!raxPrev(&di->ri))
        return NULL;
    if (keylen)
        *keylen = di->ri.key_len;
    if (dataptr)
        *dataptr = di->ri.data;
    return di->ri.key;
}

/* Like RedisModuleNextC(), but instead of returning an internally allocated
 * buffer and key length, it returns directly a module string object allocated
 * in the specified context 'ctx' (that may be NULL exactly like for the main
 * API RedisModule_CreateString).
 *
 * The returned string object should be deallocated after use, either manually
 * or by using a context that has automatic memory management active. */
RedisModuleString *RM_DictNext(RedisModuleCtx *ctx, RedisModuleDictIter *di, void **dataptr)
{
    size_t keylen;
    void *key = RM_DictNextC(di, &keylen, dataptr);
    if (key == NULL)
        return NULL;
    return RM_CreateString(ctx, key, keylen);
}

/* Like RedisModule_DictNext() but after returning the currently selected
 * element in the iterator, it selects the previous element (laxicographically
 * smaller) instead of the next one. */
RedisModuleString *RM_DictPrev(RedisModuleCtx *ctx, RedisModuleDictIter *di, void **dataptr)
{
    size_t keylen;
    void *key = RM_DictPrevC(di, &keylen, dataptr);
    if (key == NULL)
        return NULL;
    return RM_CreateString(ctx, key, keylen);
}

/* Compare the element currently pointed by the iterator to the specified
 * element given by key/keylen, according to the operator 'op' (the set of
 * valid operators are the same valid for RedisModule_DictIteratorStart).
 * If the comparision is successful the command returns REDISMODULE_OK
 * otherwise REDISMODULE_ERR is returned.
 *
 * This is useful when we want to just emit a lexicographical range, so
 * in the loop, as we iterate elements, we can also check if we are still
 * on range.
 *
 * The function returne REDISMODULE_ERR if the iterator reached the
 * end of elements condition as well. */
int RM_DictCompareC(RedisModuleDictIter *di, const char *op, void *key, size_t keylen)
{
    if (raxEOF(&di->ri))
        return REDISMODULE_ERR;
    int res = raxCompare(&di->ri, op, key, keylen);
    return res ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Like RedisModule_DictCompareC but gets the key to compare with the current
 * iterator key as a RedisModuleString. */
int RM_DictCompare(RedisModuleDictIter *di, const char *op, RedisModuleString *key)
{
    if (raxEOF(&di->ri))
        return REDISMODULE_ERR;
    int res = raxCompare(&di->ri, op, key->ptr, sdslen(key->ptr));
    return res ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* --------------------------------------------------------------------------
 * Modules utility APIs
 * -------------------------------------------------------------------------- */

/* Return random bytes using SHA1 in counter mode with a /dev/urandom
 * initialized seed. This function is fast so can be used to generate
 * many bytes without any effect on the operating system entropy pool.
 * Currently this function is not thread safe. */
void RM_GetRandomBytes(unsigned char *dst, size_t len)
{
    getRandomBytes(dst, len);
}

/* Like RedisModule_GetRandomBytes() but instead of setting the string to
 * random bytes the string is set to random characters in the in the
 * hex charset [0-9a-f]. */
void RM_GetRandomHexChars(char *dst, size_t len)
{
    getRandomHexChars(dst, len);
}

/* --------------------------------------------------------------------------
 * Modules API exporting / importing
 * -------------------------------------------------------------------------- */

/* This function is called by a module in order to export some API with a
 * given name. Other modules will be able to use this API by calling the
 * symmetrical function RM_GetSharedAPI() and casting the return value to
 * the right function pointer.
 *
 * The function will return REDISMODULE_OK if the name is not already taken,
 * otherwise REDISMODULE_ERR will be returned and no operation will be
 * performed.
 *
 * IMPORTANT: the apiname argument should be a string literal with static
 * lifetime. The API relies on the fact that it will always be valid in
 * the future. */
int RM_ExportSharedAPI(RedisModuleCtx *ctx, const char *apiname, void *func)
{
    RedisModuleSharedAPI *sapi = zmalloc(sizeof(*sapi));
    sapi->module = ctx->module;
    sapi->func = func;
    if (dictAdd(server.sharedapi, (char *)apiname, sapi) != DICT_OK)
    {
        zfree(sapi);
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

/* Request an exported API pointer. The return value is just a void pointer
 * that the caller of this function will be required to cast to the right
 * function pointer, so this is a private contract between modules.
 *
 * If the requested API is not available then NULL is returned. Because
 * modules can be loaded at different times with different order, this
 * function calls should be put inside some module generic API registering
 * step, that is called every time a module attempts to execute a
 * command that requires external APIs: if some API cannot be resolved, the
 * command should return an error.
 *
 * Here is an exmaple:
 *
 *     int ... myCommandImplementation() {
 *        if (getExternalAPIs() == 0) {
 *             reply with an error here if we cannot have the APIs
 *        }
 *        // Use the API:
 *        myFunctionPointer(foo);
 *     }
 *
 * And the function registerAPI() is:
 *
 *     int getExternalAPIs(void) {
 *         static int api_loaded = 0;
 *         if (api_loaded != 0) return 1; // APIs already resolved.
 *
 *         myFunctionPointer = RedisModule_GetOtherModuleAPI("...");
 *         if (myFunctionPointer == NULL) return 0;
 *
 *         return 1;
 *     }
 */
void *RM_GetSharedAPI(RedisModuleCtx *ctx, const char *apiname)
{
    dictEntry *de = dictFind(server.sharedapi, apiname);
    if (de == NULL)
        return NULL;
    RedisModuleSharedAPI *sapi = dictGetVal(de);
    if (listSearchKey(sapi->module->usedby, ctx->module) == NULL)
    {
        listAddNodeTail(sapi->module->usedby, ctx->module);
        listAddNodeTail(ctx->module->using, sapi->module);
    }
    return sapi->func;
}

/* Remove all the APIs registered by the specified module. Usually you
 * want this when the module is going to be unloaded. This function
 * assumes that's caller responsibility to make sure the APIs are not
 * used by other modules.
 *
 * The number of unregistered APIs is returned. */
int moduleUnregisterSharedAPI(RedisModule *module)
{
    int count = 0;
    dictIterator *di = dictGetSafeIterator(server.sharedapi);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL)
    {
        const char *apiname = dictGetKey(de);
        RedisModuleSharedAPI *sapi = dictGetVal(de);
        if (sapi->module == module)
        {
            dictDelete(server.sharedapi, apiname);
            zfree(sapi);
            count++;
        }
    }
    dictReleaseIterator(di);
    return count;
}

/* Remove the specified module as an user of APIs of ever other module.
 * This is usually called when a module is unloaded.
 *
 * Returns the number of modules this module was using APIs from. */
int moduleUnregisterUsedAPI(RedisModule *module)
{
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(module->using, &li);
    while ((ln = listNext(&li)))
    {
        RedisModule *used = ln->value;
        listNode *ln = listSearchKey(used->usedby, module);
        if (ln)
        {
            listDelNode(module->using, ln);
            count++;
        }
    }
    return count;
}

/* Unregister all filters registered by a module.
 * This is called when a module is being unloaded.
 *
 * Returns the number of filters unregistered. */
int moduleUnregisterFilters(RedisModule *module)
{
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(module->filters, &li);
    while ((ln = listNext(&li)))
    {
        RedisModuleCommandFilter *filter = ln->value;
        listNode *ln = listSearchKey(moduleCommandFilters, filter);
        if (ln)
        {
            listDelNode(moduleCommandFilters, ln);
            count++;
        }
        zfree(filter);
    }
    return count;
}

/* --------------------------------------------------------------------------
 * Module Command Filter API
 * -------------------------------------------------------------------------- */

/* Register a new command filter function.
 *
 * Command filtering makes it possible for modules to extend Redis by plugging
 * into the execution flow of all commands.
 *
 * A registered filter gets called before Redis executes *any* command.  This
 * includes both core Redis commands and commands registered by any module.  The
 * filter applies in all execution paths including:
 *
 * 1. Invocation by a client.
 * 2. Invocation through `RedisModule_Call()` by any module.
 * 3. Invocation through Lua 'redis.call()`.
 * 4. Replication of a command from a master.
 *
 * The filter executes in a special filter context, which is different and more
 * limited than a RedisModuleCtx.  Because the filter affects any command, it
 * must be implemented in a very efficient way to reduce the performance impact
 * on Redis.  All Redis Module API calls that require a valid context (such as
 * `RedisModule_Call()`, `RedisModule_OpenKey()`, etc.) are not supported in a
 * filter context.
 *
 * The `RedisModuleCommandFilterCtx` can be used to inspect or modify the
 * executed command and its arguments.  As the filter executes before Redis
 * begins processing the command, any change will affect the way the command is
 * processed.  For example, a module can override Redis commands this way:
 *
 * 1. Register a `MODULE.SET` command which implements an extended version of
 *    the Redis `SET` command.
 * 2. Register a command filter which detects invocation of `SET` on a specific
 *    pattern of keys.  Once detected, the filter will replace the first
 *    argument from `SET` to `MODULE.SET`.
 * 3. When filter execution is complete, Redis considers the new command name
 *    and therefore executes the module's own command.
 *
 * Note that in the above use case, if `MODULE.SET` itself uses
 * `RedisModule_Call()` the filter will be applied on that call as well.  If
 * that is not desired, the `REDISMODULE_CMDFILTER_NOSELF` flag can be set when
 * registering the filter.
 *
 * The `REDISMODULE_CMDFILTER_NOSELF` flag prevents execution flows that
 * originate from the module's own `RM_Call()` from reaching the filter.  This
 * flag is effective for all execution flows, including nested ones, as long as
 * the execution begins from the module's command context or a thread-safe
 * context that is associated with a blocking command.
 *
 * Detached thread-safe contexts are *not* associated with the module and cannot
 * be protected by this flag.
 *
 * If multiple filters are registered (by the same or different modules), they
 * are executed in the order of registration.
 */

RedisModuleCommandFilter *RM_RegisterCommandFilter(RedisModuleCtx *ctx, RedisModuleCommandFilterFunc callback, int flags)
{
    RedisModuleCommandFilter *filter = zmalloc(sizeof(*filter));
    filter->module = ctx->module;
    filter->callback = callback;
    filter->flags = flags;

    listAddNodeTail(moduleCommandFilters, filter);
    listAddNodeTail(ctx->module->filters, filter);
    return filter;
}

/* Unregister a command filter.
 */
int RM_UnregisterCommandFilter(RedisModuleCtx *ctx, RedisModuleCommandFilter *filter)
{
    listNode *ln;

    /* A module can only remove its own filters */
    if (filter->module != ctx->module)
        return REDISMODULE_ERR;

    ln = listSearchKey(moduleCommandFilters, filter);
    if (!ln)
        return REDISMODULE_ERR;
    listDelNode(moduleCommandFilters, ln);

    ln = listSearchKey(ctx->module->filters, filter);
    if (!ln)
        return REDISMODULE_ERR; /* Shouldn't happen */
    listDelNode(ctx->module->filters, ln);

    return REDISMODULE_OK;
}

void moduleCallCommandFilters(client *c)
{
    if (listLength(moduleCommandFilters) == 0)
    {
        return;
    }

    listIter li;
    listNode *ln;
    listRewind(moduleCommandFilters, &li);

    RedisModuleCommandFilterCtx filter = {
        .argv = c->argv,
        .argc = c->argc};

    while ((ln = listNext(&li)))
    {
        RedisModuleCommandFilter *f = ln->value;

        /* Skip filter if REDISMODULE_CMDFILTER_NOSELF is set and module is
         * currently processing a command.
         */
        if ((f->flags & REDISMODULE_CMDFILTER_NOSELF) && f->module->in_call)
            continue;

        /* Call filter */
        f->callback(&filter);
    }

    c->argv = filter.argv;
    c->argc = filter.argc;
}

/* Return the number of arguments a filtered command has.  The number of
 * arguments include the command itself.
 */
int RM_CommandFilterArgsCount(RedisModuleCommandFilterCtx *fctx)
{
    return fctx->argc;
}

/* Return the specified command argument.  The first argument (position 0) is
 * the command itself, and the rest are user-provided args.
 */
const RedisModuleString *RM_CommandFilterArgGet(RedisModuleCommandFilterCtx *fctx, int pos)
{
    if (pos < 0 || pos >= fctx->argc)
        return NULL;
    return fctx->argv[pos];
}

/* Modify the filtered command by inserting a new argument at the specified
 * position.  The specified RedisModuleString argument may be used by Redis
 * after the filter context is destroyed, so it must not be auto-memory
 * allocated, freed or used elsewhere.
 */

int RM_CommandFilterArgInsert(RedisModuleCommandFilterCtx *fctx, int pos, RedisModuleString *arg)
{
    int i;

    if (pos < 0 || pos > fctx->argc)
        return REDISMODULE_ERR;

    fctx->argv = zrealloc(fctx->argv, (fctx->argc + 1) * sizeof(RedisModuleString *));
    for (i = fctx->argc; i > pos; i--)
    {
        fctx->argv[i] = fctx->argv[i - 1];
    }
    fctx->argv[pos] = arg;
    fctx->argc++;

    return REDISMODULE_OK;
}

/* Modify the filtered command by replacing an existing argument with a new one.
 * The specified RedisModuleString argument may be used by Redis after the
 * filter context is destroyed, so it must not be auto-memory allocated, freed
 * or used elsewhere.
 */

int RM_CommandFilterArgReplace(RedisModuleCommandFilterCtx *fctx, int pos, RedisModuleString *arg)
{
    if (pos < 0 || pos >= fctx->argc)
        return REDISMODULE_ERR;

    decrRefCount(fctx->argv[pos]);
    fctx->argv[pos] = arg;

    return REDISMODULE_OK;
}

/* Modify the filtered command by deleting an argument at the specified
 * position.
 */
int RM_CommandFilterArgDelete(RedisModuleCommandFilterCtx *fctx, int pos)
{
    int i;
    if (pos < 0 || pos >= fctx->argc)
        return REDISMODULE_ERR;

    decrRefCount(fctx->argv[pos]);
    for (i = pos; i < fctx->argc - 1; i++)
    {
        fctx->argv[i] = fctx->argv[i + 1];
    }
    fctx->argc--;

    return REDISMODULE_OK;
}

/* --------------------------------------------------------------------------
 * Modules API internals
 * -------------------------------------------------------------------------- */

/* server.moduleapi dictionary type. Only uses plain C strings since
 * this gets queries from modules. */

uint64_t dictCStringKeyHash(const void *key)
{
    return dictGenHashFunction((unsigned char *)key, strlen((char *)key));
}

int dictCStringKeyCompare(void *privdata, const void *key1, const void *key2)
{
    UNUSED(privdata);
    return strcmp(key1, key2) == 0;
}

dictType moduleAPIDictType = {
    dictCStringKeyHash,    /* hash function */
    NULL,                  /* key dup */
    NULL,                  /* val dup */
    dictCStringKeyCompare, /* key compare */
    NULL,                  /* key destructor */
    NULL                   /* val destructor */
};

int moduleRegisterApi(const char *funcname, void *funcptr)
{
    return dictAdd(server.moduleapi, (char *)funcname, funcptr);
}

#define REGISTER_API(name) \
    moduleRegisterApi("RedisModule_" #name, (void *)(unsigned long)RM_##name)

/* Global initialization at Redis startup. */
void moduleRegisterCoreAPI(void);

void moduleInitModulesSystem(void)
{
    moduleUnblockedClients = listCreate();
    server.loadmodule_queue = listCreate();
    modules = dictCreate(&modulesDictType, NULL);

    /* Set up the keyspace notification susbscriber list and static client */
    moduleKeyspaceSubscribers = listCreate();
    moduleFreeContextReusedClient = createClient(-1);
    moduleFreeContextReusedClient->flags |= CLIENT_MODULE;

    /* Set up filter list */
    moduleCommandFilters = listCreate();

    moduleRegisterCoreAPI();
    if (pipe(server.module_blocked_pipe) == -1)
    {
        serverLog(LL_WARNING,
                  "Can't create the pipe for module blocking commands: %s",
                  strerror(errno));
        exit(1);
    }
    /* Make the pipe non blocking. This is just a best effort aware mechanism
     * and we do not want to block not in the read nor in the write half. */
    anetNonBlock(NULL, server.module_blocked_pipe[0]);
    anetNonBlock(NULL, server.module_blocked_pipe[1]);

    /* Create the timers radix tree. */
    Timers = raxNew();

    /* Our thread-safe contexts GIL must start with already locked:
     * it is just unlocked when it's safe. */
    pthread_mutex_lock(&moduleGIL);
}

/* Load all the modules in the server.loadmodule_queue list, which is
 * populated by `loadmodule` directives in the configuration file.
 * We can't load modules directly when processing the configuration file
 * because the server must be fully initialized before loading modules.
 *
 * The function aborts the server on errors, since to start with missing
 * modules is not considered sane: clients may rely on the existence of
 * given commands, loading AOF also may need some modules to exist, and
 * if this instance is a slave, it must understand commands from master. */
void moduleLoadFromQueue(void)
{
    listIter li;
    listNode *ln;

    listRewind(server.loadmodule_queue, &li);
    while ((ln = listNext(&li)))
    {
        struct moduleLoadQueueEntry *loadmod = ln->value;
        if (moduleLoad(loadmod->path, (void **)loadmod->argv, loadmod->argc) == C_ERR)
        {
            serverLog(LL_WARNING,
                      "Can't load module from %s: server aborting",
                      loadmod->path);
            exit(1);
        }
    }
}

void moduleFreeModuleStructure(struct RedisModule *module)
{
    listRelease(module->types);
    listRelease(module->filters);
    sdsfree(module->name);
    zfree(module);
}

void moduleUnregisterCommands(struct RedisModule *module)
{
    /* Unregister all the commands registered by this module. */
    dictIterator *di = dictGetSafeIterator(server.commands);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL)
    {
        struct redisCommand *cmd = dictGetVal(de);
        if (cmd->proc == RedisModuleCommandDispatcher)
        {
            RedisModuleCommandProxy *cp =
                (void *)(unsigned long)cmd->getkeys_proc;
            sds cmdname = cp->rediscmd->name;
            if (cp->module == module)
            {
                dictDelete(server.commands, cmdname);
                dictDelete(server.orig_commands, cmdname);
                sdsfree(cmdname);
                zfree(cp->rediscmd);
                zfree(cp);
            }
        }
    }
    dictReleaseIterator(di);
}

/* Load a module and initialize it. On success C_OK is returned, otherwise
 * C_ERR is returned. */
int moduleLoad(const char *path, void **module_argv, int module_argc)
{
    int (*onload)(void *, void **, int);
    void *handle;
    RedisModuleCtx ctx = REDISMODULE_CTX_INIT;

    handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL)
    {
        serverLog(LL_WARNING, "Module %s failed to load: %s", path, dlerror());
        return C_ERR;
    }
    onload = (int (*)(void *, void **, int))(unsigned long)dlsym(handle, "RedisModule_OnLoad");
    if (onload == NULL)
    {
        dlclose(handle);
        serverLog(LL_WARNING,
                  "Module %s does not export RedisModule_OnLoad() "
                  "symbol. Module not loaded.",
                  path);
        return C_ERR;
    }
    if (onload((void *)&ctx, module_argv, module_argc) == REDISMODULE_ERR)
    {
        if (ctx.module)
        {
            moduleUnregisterCommands(ctx.module);
            moduleUnregisterSharedAPI(ctx.module);
            moduleUnregisterUsedAPI(ctx.module);
            moduleFreeModuleStructure(ctx.module);
        }
        dlclose(handle);
        serverLog(LL_WARNING,
                  "Module %s initialization failed. Module not loaded", path);
        return C_ERR;
    }

    /* Redis module loaded! Register it. */
    dictAdd(modules, ctx.module->name, ctx.module);
    ctx.module->handle = handle;
    serverLog(LL_NOTICE, "Module '%s' loaded from %s", ctx.module->name, path);
    moduleFreeContext(&ctx);
    return C_OK;
}

/* Unload the module registered with the specified name. On success
 * C_OK is returned, otherwise C_ERR is returned and errno is set
 * to the following values depending on the type of error:
 *
 * * ENONET: No such module having the specified name.
 * * EBUSY: The module exports a new data type and can only be reloaded. */
int moduleUnload(sds name)
{
    struct RedisModule *module = dictFetchValue(modules, name);

    if (module == NULL)
    {
        errno = ENOENT;
        return REDISMODULE_ERR;
    }
    else if (listLength(module->types))
    {
        errno = EBUSY;
        return REDISMODULE_ERR;
    }
    else if (listLength(module->usedby))
    {
        errno = EPERM;
        return REDISMODULE_ERR;
    }

    moduleUnregisterCommands(module);
    moduleUnregisterSharedAPI(module);
    moduleUnregisterUsedAPI(module);
    moduleUnregisterFilters(module);

    /* Remove any notification subscribers this module might have */
    moduleUnsubscribeNotifications(module);

    /* Unregister all the hooks. TODO: Yet no hooks support here. */

    /* Unload the dynamic library. */
    if (dlclose(module->handle) == -1)
    {
        char *error = dlerror();
        if (error == NULL)
            error = "Unknown error";
        serverLog(LL_WARNING, "Error when trying to close the %s module: %s",
                  module->name, error);
    }

    /* Remove from list of modules. */
    serverLog(LL_NOTICE, "Module %s unloaded", module->name);
    dictDelete(modules, module->name);
    module->name = NULL; /* The name was already freed by dictDelete(). */
    moduleFreeModuleStructure(module);

    return REDISMODULE_OK;
}

/* Redis MODULE command.
 *
 * MODULE LOAD <path> [args...] */
void moduleCommand(client *c)
{
    char *subcmd = c->argv[1]->ptr;
    if (c->argc == 2 && !strcasecmp(subcmd, "help"))
    {
        const char *help[] = {
            "LIST -- Return a list of loaded modules.",
            "LOAD <path> [arg ...] -- Load a module library from <path>.",
            "UNLOAD <name> -- Unload a module.",
            NULL};
        addReplyHelp(c, help);
    }
    else if (!strcasecmp(subcmd, "load") && c->argc >= 3)
    {
        robj **argv = NULL;
        int argc = 0;

        if (c->argc > 3)
        {
            argc = c->argc - 3;
            argv = &c->argv[3];
        }

        if (moduleLoad(c->argv[2]->ptr, (void **)argv, argc) == C_OK)
            addReply(c, shared.ok);
        else
            addReplyError(c,
                          "Error loading the extension. Please check the server logs.");
    }
    else if (!strcasecmp(subcmd, "unload") && c->argc == 3)
    {
        if (moduleUnload(c->argv[2]->ptr) == C_OK)
            addReply(c, shared.ok);
        else
        {
            char *errmsg;
            switch (errno)
            {
            case ENOENT:
                errmsg = "no such module with that name";
                break;
            case EBUSY:
                errmsg = "the module exports one or more module-side data "
                         "types, can't unload";
                break;
            case EPERM:
                errmsg = "the module exports APIs used by other modules. "
                         "Please unload them first and try again";
                break;
            default:
                errmsg = "operation not possible.";
                break;
            }
            addReplyErrorFormat(c, "Error unloading module: %s", errmsg);
        }
    }
    else if (!strcasecmp(subcmd, "list") && c->argc == 2)
    {
        dictIterator *di = dictGetIterator(modules);
        dictEntry *de;

        addReplyMultiBulkLen(c, dictSize(modules));
        while ((de = dictNext(di)) != NULL)
        {
            sds name = dictGetKey(de);
            struct RedisModule *module = dictGetVal(de);
            addReplyMultiBulkLen(c, 4);
            addReplyBulkCString(c, "name");
            addReplyBulkCBuffer(c, name, sdslen(name));
            addReplyBulkCString(c, "ver");
            addReplyLongLong(c, module->ver);
        }
        dictReleaseIterator(di);
    }
    else
    {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* Return the number of registered modules. */
size_t moduleCount(void)
{
    return dictSize(modules);
}

/* Register all the APIs we export. Keep this function at the end of the
 * file so that's easy to seek it to add new entries. */
void moduleRegisterCoreAPI(void)
{
    server.moduleapi = dictCreate(&moduleAPIDictType, NULL);
    server.sharedapi = dictCreate(&moduleAPIDictType, NULL);
    REGISTER_API(Alloc);
    REGISTER_API(Calloc);
    REGISTER_API(Realloc);
    REGISTER_API(Free);
    REGISTER_API(Strdup);
    REGISTER_API(CreateCommand);
    REGISTER_API(SetModuleAttribs);
    REGISTER_API(IsModuleNameBusy);
    REGISTER_API(WrongArity);
    REGISTER_API(ReplyWithLongLong);
    REGISTER_API(ReplyWithError);
    REGISTER_API(ReplyWithSimpleString);
    REGISTER_API(ReplyWithArray);
    REGISTER_API(ReplySetArrayLength);
    REGISTER_API(ReplyWithString);
    REGISTER_API(ReplyWithStringBuffer);
    REGISTER_API(ReplyWithCString);
    REGISTER_API(ReplyWithNull);
    REGISTER_API(ReplyWithCallReply);
    REGISTER_API(ReplyWithDouble);
    REGISTER_API(GetSelectedDb);
    REGISTER_API(SelectDb);
    REGISTER_API(OpenKey);
    REGISTER_API(CloseKey);
    REGISTER_API(KeyType);
    REGISTER_API(ValueLength);
    REGISTER_API(ListPush);
    REGISTER_API(ListPop);
    REGISTER_API(StringToLongLong);
    REGISTER_API(StringToDouble);
    REGISTER_API(Call);
    REGISTER_API(CallReplyProto);
    REGISTER_API(FreeCallReply);
    REGISTER_API(CallReplyInteger);
    REGISTER_API(CallReplyType);
    REGISTER_API(CallReplyLength);
    REGISTER_API(CallReplyArrayElement);
    REGISTER_API(CallReplyStringPtr);
    REGISTER_API(CreateStringFromCallReply);
    REGISTER_API(CreateString);
    REGISTER_API(CreateStringFromLongLong);
    REGISTER_API(CreateStringFromString);
    REGISTER_API(CreateStringPrintf);
    REGISTER_API(FreeString);
    REGISTER_API(StringPtrLen);
    REGISTER_API(AutoMemory);
    REGISTER_API(Replicate);
    REGISTER_API(ReplicateVerbatim);
    REGISTER_API(DeleteKey);
    REGISTER_API(UnlinkKey);
    REGISTER_API(StringSet);
    REGISTER_API(StringDMA);
    REGISTER_API(StringTruncate);
    REGISTER_API(SetExpire);
    REGISTER_API(GetExpire);
    REGISTER_API(ZsetAdd);
    REGISTER_API(ZsetIncrby);
    REGISTER_API(ZsetScore);
    REGISTER_API(ZsetRem);
    REGISTER_API(ZsetRangeStop);
    REGISTER_API(ZsetFirstInScoreRange);
    REGISTER_API(ZsetLastInScoreRange);
    REGISTER_API(ZsetFirstInLexRange);
    REGISTER_API(ZsetLastInLexRange);
    REGISTER_API(ZsetRangeCurrentElement);
    REGISTER_API(ZsetRangeNext);
    REGISTER_API(ZsetRangePrev);
    REGISTER_API(ZsetRangeEndReached);
    REGISTER_API(HashSet);
    REGISTER_API(HashGet);
    REGISTER_API(IsKeysPositionRequest);
    REGISTER_API(KeyAtPos);
    REGISTER_API(GetClientId);
    REGISTER_API(GetContextFlags);
    REGISTER_API(PoolAlloc);
    REGISTER_API(CreateDataType);
    REGISTER_API(ModuleTypeSetValue);
    REGISTER_API(ModuleTypeGetType);
    REGISTER_API(ModuleTypeGetValue);
    REGISTER_API(SaveUnsigned);
    REGISTER_API(LoadUnsigned);
    REGISTER_API(SaveSigned);
    REGISTER_API(LoadSigned);
    REGISTER_API(SaveString);
    REGISTER_API(SaveStringBuffer);
    REGISTER_API(LoadString);
    REGISTER_API(LoadStringBuffer);
    REGISTER_API(SaveDouble);
    REGISTER_API(LoadDouble);
    REGISTER_API(SaveFloat);
    REGISTER_API(LoadFloat);
    REGISTER_API(EmitAOF);
    REGISTER_API(Log);
    REGISTER_API(LogIOError);
    REGISTER_API(StringAppendBuffer);
    REGISTER_API(RetainString);
    REGISTER_API(StringCompare);
    REGISTER_API(GetContextFromIO);
    REGISTER_API(GetKeyNameFromIO);
    REGISTER_API(BlockClient);
    REGISTER_API(UnblockClient);
    REGISTER_API(IsBlockedReplyRequest);
    REGISTER_API(IsBlockedTimeoutRequest);
    REGISTER_API(GetBlockedClientPrivateData);
    REGISTER_API(AbortBlock);
    REGISTER_API(Milliseconds);
    REGISTER_API(GetThreadSafeContext);
    REGISTER_API(FreeThreadSafeContext);
    REGISTER_API(ThreadSafeContextLock);
    REGISTER_API(ThreadSafeContextUnlock);
    REGISTER_API(DigestAddStringBuffer);
    REGISTER_API(DigestAddLongLong);
    REGISTER_API(DigestEndSequence);
    REGISTER_API(SubscribeToKeyspaceEvents);
    REGISTER_API(RegisterClusterMessageReceiver);
    REGISTER_API(SendClusterMessage);
    REGISTER_API(GetClusterNodeInfo);
    REGISTER_API(GetClusterNodesList);
    REGISTER_API(FreeClusterNodesList);
    REGISTER_API(CreateTimer);
    REGISTER_API(StopTimer);
    REGISTER_API(GetTimerInfo);
    REGISTER_API(GetMyClusterID);
    REGISTER_API(GetClusterSize);
    REGISTER_API(GetRandomBytes);
    REGISTER_API(GetRandomHexChars);
    REGISTER_API(BlockedClientDisconnected);
    REGISTER_API(SetDisconnectCallback);
    REGISTER_API(GetBlockedClientHandle);
    REGISTER_API(SetClusterFlags);
    REGISTER_API(CreateDict);
    REGISTER_API(FreeDict);
    REGISTER_API(DictSize);
    REGISTER_API(DictSetC);
    REGISTER_API(DictReplaceC);
    REGISTER_API(DictSet);
    REGISTER_API(DictReplace);
    REGISTER_API(DictGetC);
    REGISTER_API(DictGet);
    REGISTER_API(DictDelC);
    REGISTER_API(DictDel);
    REGISTER_API(DictIteratorStartC);
    REGISTER_API(DictIteratorStart);
    REGISTER_API(DictIteratorStop);
    REGISTER_API(DictIteratorReseekC);
    REGISTER_API(DictIteratorReseek);
    REGISTER_API(DictNextC);
    REGISTER_API(DictPrevC);
    REGISTER_API(DictNext);
    REGISTER_API(DictPrev);
    REGISTER_API(DictCompareC);
    REGISTER_API(DictCompare);
    REGISTER_API(ExportSharedAPI);
    REGISTER_API(GetSharedAPI);
    REGISTER_API(RegisterCommandFilter);
    REGISTER_API(UnregisterCommandFilter);
    REGISTER_API(CommandFilterArgsCount);
    REGISTER_API(CommandFilterArgGet);
    REGISTER_API(CommandFilterArgInsert);
    REGISTER_API(CommandFilterArgReplace);
    REGISTER_API(CommandFilterArgDelete);
}
