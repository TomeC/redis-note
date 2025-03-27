/* SDSLib 2.0 -- A C dynamic strings library
 *
 * Copyright (c) 2006-2015, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2015, Oran Agra
 * Copyright (c) 2015, Redis Labs, Inc
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
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <limits.h>
#include "sds.h"
#include "sdsalloc.h"

const char *SDS_NOINIT = "SDS_NOINIT";
// 结构体大小
static inline int sdsHdrSize(char type)
{
    switch (type & SDS_TYPE_MASK) // &111
    {
    case SDS_TYPE_5: // 不会存在
        return sizeof(struct sdshdr5);
    case SDS_TYPE_8:
        return sizeof(struct sdshdr8);
    case SDS_TYPE_16:
        return sizeof(struct sdshdr16);
    case SDS_TYPE_32:
        return sizeof(struct sdshdr32);
    case SDS_TYPE_64:
        return sizeof(struct sdshdr64);
    }
    return 0;
}
// 根据字符串长度选择合适的sds类型
static inline char sdsReqType(size_t string_size)
{
    if (string_size < 1 << 5)
    { // 2^5 = 32
        return SDS_TYPE_5;
    }
    if (string_size < 1 << 8)
    { // 256
        return SDS_TYPE_8;
    }
    if (string_size < 1 << 16)
    { // 65536
        return SDS_TYPE_16;
    }

#if (LONG_MAX == LLONG_MAX) // 64位系统
    if (string_size < 1ll << 32)
    {
        // 长度小于2^32位 长度42亿
        return SDS_TYPE_32;
    }
    return SDS_TYPE_64;
#else
    // 32位系统直接返回
    return SDS_TYPE_32;
#endif
}

/* Create a new sds string with the content specified by the 'init' pointer
 * and 'initlen'.
 * If NULL is used for 'init' the string is initialized with zero bytes.
 * If SDS_NOINIT is used, the buffer is left uninitialized;
 *
 * The string is always null-termined (all the sds strings are, always) so
 * even if you create an sds string with:
 *
 * mystring = sdsnewlen("abc",3);
 *
 * You can print the string with printf() as there is an implicit \0 at the
 * end of the string. However the string is binary safe and can contain
 * \0 characters in the middle, as the length is stored in the sds header. */
sds sdsnewlen(const void *initp, size_t initlen)
{
    void *sh;                        // 结构体的起始位置
    sds s;                           // sds = char * 结构体字符串的指针
    char type = sdsReqType(initlen); // 根据字符串的长度获取适合的hdr
    // 初始化长度为0的sds通常是为了追加数据，直接使用SDS_TYPE_8
    if (type == SDS_TYPE_5 && initlen == 0)
    {
        type = SDS_TYPE_8;
    }
    int hdrlen = sdsHdrSize(type);
    unsigned char *fp; /* flags pointer. */

    sh = s_malloc(hdrlen + initlen + 1); // +1 是为了存'\0'
    if (initp == SDS_NOINIT)
    { // 直接==比较字符串要保证不是malloc出来的
        initp = NULL;
    }
    else if (!initp)
    {
        // initp为null时初始化全部内存空间为0，不必初始化浪费时间，直接首位设置为\0就可以
        memset(sh, 0, hdrlen + initlen + 1);
    }
    if (sh == NULL)
    { // 这个判断不必要s_malloc分配失败会调用abort?
        return NULL;
    }
    s = (char *)sh + hdrlen;       // 指向数据字符串
    fp = ((unsigned char *)s) - 1; // 后退一位指向flags
    switch (type)
    {
    case SDS_TYPE_5:
    {
        // 不存在
        // 数据长度小于32,initlen最大 1,1111，右移3位，共8位。fp前5位是数据长度
        *fp = type | (initlen << SDS_TYPE_BITS);
        break;
    }
    case SDS_TYPE_8:
    {
        SDS_HDR_VAR(8, s); // 从s里获取hdr的指针
        sh->len = initlen;
        sh->alloc = initlen;
        *fp = type;
        break;
    }
    case SDS_TYPE_16:
    {
        SDS_HDR_VAR(16, s);
        sh->len = initlen;
        sh->alloc = initlen;
        *fp = type;
        break;
    }
    case SDS_TYPE_32:
    {
        SDS_HDR_VAR(32, s);
        sh->len = initlen;
        sh->alloc = initlen;
        *fp = type;
        break;
    }
    case SDS_TYPE_64:
    {
        SDS_HDR_VAR(64, s);
        sh->len = initlen;
        sh->alloc = initlen;
        *fp = type;
        break;
    }
    }
    if (initlen && initp)
    {
        memcpy(s, initp, initlen);
    }
    s[initlen] = '\0'; // 防止initlen长度不够溢出
    return s;
}

/* Create an empty (zero length) sds string. Even in this case the string
 * always has an implicit null term. */
sds sdsempty(void)
{
    return sdsnewlen("", 0);
}

/* Create a new sds string starting from a null terminated C string. */
sds sdsnew(const char *init)
{
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

/* 赋值字符串 */
sds sdsdup(const sds s)
{
    return sdsnewlen(s, sdslen(s));
}

/* Free an sds string. No operation is performed if 's' is NULL. */
void sdsfree(sds s)
{
    if (s == NULL)
        return;
    s_free((char *)s - sdsHdrSize(s[-1])); // 后退到结构体的起始指针
}

/* Set the sds string length to the length as obtained with strlen(), so
 * considering as content only up to the first null term character.
 *
 * This function is useful when the sds string is hacked manually in some
 * way, like in the following example:
 *
 * s = sdsnew("foobar");
 * s[2] = '\0';
 * sdsupdatelen(s);// 如果注释掉，输出就会变成6
 * printf("%d\n", sdslen(s));
 *
 * The output will be "2", but if we comment out the call to sdsupdatelen()
 * the output will be "6" as the string was modified but the logical length
 * remains 6 bytes. */
void sdsupdatelen(sds s)
{
    size_t reallen = strlen(s);
    sdssetlen(s, reallen);
}

/* Modify an sds string in-place to make it empty (zero length).
 * However all the existing buffer is not discarded but set as free space
 * so that next append operations will not require allocations up to the
 * number of bytes previously available. */
void sdsclear(sds s)
{
    sdssetlen(s, 0);
    s[0] = '\0';
}

/* Enlarge扩大 the free space at the end of the sds string so that the caller
 * is sure that after calling this function can overwrite up to addlen
 * bytes after the end of the string, plus one more byte for nul term.
 *
 * Note: this does not change the *length* of the sds string as returned
 * by sdslen(), but only the free buffer space we have. */
sds sdsMakeRoomFor(sds s, size_t addlen)
{ // s的长度+addlen
    void *sh, *newsh;
    size_t avail = sdsavail(s); // 剩余可用空间长度
    size_t len, newlen;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen;

    if (avail >= addlen)
    {
        // 剩余空间足够，不需要扩容
        return s;
    }
    len = sdslen(s);
    sh = (char *)s - sdsHdrSize(oldtype);
    newlen = (len + addlen);
    if (newlen < SDS_MAX_PREALLOC)
    {
        newlen *= 2; // 小于1M，直接翻倍
    }
    else
    {
        newlen += SDS_MAX_PREALLOC; // 否则扩容长度加预留1M长度
    }

    type = sdsReqType(newlen);

    if (type == SDS_TYPE_5)
    {
        // 扩容不允许使用SDS_TYPE_5，因为它无法扩容
        type = SDS_TYPE_8;
    }

    hdrlen = sdsHdrSize(type);
    if (oldtype == type)
    {
        // 类型不变，直接realloc
        newsh = s_realloc(sh, hdrlen + newlen + 1);
        if (newsh == NULL)
        {
            return NULL;
        }
        s = (char *)newsh + hdrlen;
    }
    else
    {
        // 类型变了，重新申请空间，旧数据复制过去
        newsh = s_malloc(hdrlen + newlen + 1);
        if (newsh == NULL)
        {
            return NULL;
        }
        memcpy((char *)newsh + hdrlen, s, len + 1);
        s_free(sh);
        s = (char *)newsh + hdrlen;
        s[-1] = type;
        sdssetlen(s, len);
    }
    sdssetalloc(s, newlen);
    return s;
}

/* Reallocate the sds string so that it has no free space at the end. The
 * contained string remains not altered, but next concatenation operations
 * will require a reallocation.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
// 压缩多余的空间
sds sdsRemoveFreeSpace(sds s)
{
    void *sh, *newsh;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen, oldhdrlen = sdsHdrSize(oldtype);
    size_t len = sdslen(s);
    size_t avail = sdsavail(s);
    sh = (char *)s - oldhdrlen;

    /* Return ASAP if there is no space left. */
    if (avail == 0)
        return s;

    /* Check what would be the minimum SDS header that is just good enough to
     * fit this string. */
    type = sdsReqType(len);
    hdrlen = sdsHdrSize(type);

    /* If the type is the same, or at least a large enough type is still
     * required, we just realloc(), letting the allocator to do the copy
     * only if really needed. Otherwise if the change is huge, we manually
     * reallocate the string to use the different header type. */
    if (oldtype == type || type > SDS_TYPE_8)
    {
        newsh = s_realloc(sh, oldhdrlen + len + 1);
        if (newsh == NULL)
            return NULL;
        s = (char *)newsh + oldhdrlen;
    }
    else
    {
        // 如果type变了或type=SDS_TYPE_5
        // 重新创建内存空间，复制原来的数据，释放原空间，
        newsh = s_malloc(hdrlen + len + 1);
        if (newsh == NULL)
            return NULL;
        memcpy((char *)newsh + hdrlen, s, len + 1);
        s_free(sh);
        s = (char *)newsh + hdrlen;
        s[-1] = type;
        sdssetlen(s, len); // 设置字符串长度
    }
    sdssetalloc(s, len); // 设置空间长度
    return s;
}

/* Return the total size of the allocation of the specified sds string,
 * including:
 * 1) The sds header before the pointer.
 * 2) The string.
 * 3) The free buffer at the end if any.
 * 4) The implicit null term.
 */
// 返回sds的总长度
size_t sdsAllocSize(sds s)
{
    size_t alloc = sdsalloc(s);
    return sdsHdrSize(s[-1]) + alloc + 1;
}

/* Return the pointer of the actual SDS allocation (normally SDS strings
 * are referenced by the start of the string buffer). */
// 保存数据的起始指针
void *sdsAllocPtr(sds s)
{
    return (void *)(s - sdsHdrSize(s[-1]));
}

/* Increment the sds length and decrements the left free space at the
 * end of the string according to 'incr'. Also set the null term
 * in the new end of the string.
 *
 * This function is used in order to fix the string length after the
 * user calls sdsMakeRoomFor(), writes something after the end of
 * the current string, and finally needs to set the new length.
 *
 * Note: it is possible to use a negative increment in order to
 * right-trim the string.
 *
 * Usage example:
 *
 * Using sdsIncrLen() and sdsMakeRoomFor() it is possible to mount the
 * following schema, to cat bytes coming from the kernel to the end of an
 * sds string without copying into an intermediate buffer:
 *
 * oldlen = sdslen(s);
 * s = sdsMakeRoomFor(s, BUFFER_SIZE);
 * nread = read(fd, s+oldlen, BUFFER_SIZE);
 * ... check for nread <= 0 and handle it ...
 * sdsIncrLen(s, nread);
 */
void sdsIncrLen(sds s, ssize_t incr)
{
    unsigned char flags = s[-1];
    size_t len;
    switch (flags & SDS_TYPE_MASK)
    {
    case SDS_TYPE_5:
    {
        unsigned char *fp = ((unsigned char *)s) - 1;
        unsigned char oldlen = SDS_TYPE_5_LEN(flags);
        assert((incr > 0 && oldlen + incr < 32) || (incr < 0 && oldlen >= (unsigned int)(-incr)));
        *fp = SDS_TYPE_5 | ((oldlen + incr) << SDS_TYPE_BITS);
        len = oldlen + incr;
        break;
    }
    case SDS_TYPE_8:
    {
        SDS_HDR_VAR(8, s);
        assert((incr >= 0 && sh->alloc - sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
        len = (sh->len += incr);
        break;
    }
    case SDS_TYPE_16:
    {
        SDS_HDR_VAR(16, s);
        assert((incr >= 0 && sh->alloc - sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
        len = (sh->len += incr);
        break;
    }
    case SDS_TYPE_32:
    {
        SDS_HDR_VAR(32, s);
        assert((incr >= 0 && sh->alloc - sh->len >= (unsigned int)incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
        len = (sh->len += incr);
        break;
    }
    case SDS_TYPE_64:
    {
        SDS_HDR_VAR(64, s);
        assert((incr >= 0 && sh->alloc - sh->len >= (uint64_t)incr) || (incr < 0 && sh->len >= (uint64_t)(-incr)));
        len = (sh->len += incr);
        break;
    }
    default:
        len = 0; /* Just to avoid compilation warnings. */
    }
    s[len] = '\0';
}

/* Grow the sds to have the specified length. Bytes that were not part of
 * the original length of the sds will be set to zero.
 *
 * if the specified length is smaller than the current length, no operation
 * is performed. */
sds sdsgrowzero(sds s, size_t len)
{
    size_t curlen = sdslen(s);

    if (len <= curlen)
        return s;
    s = sdsMakeRoomFor(s, len - curlen);
    if (s == NULL)
        return NULL;

    /* Make sure added region doesn't contain garbage */
    memset(s + curlen, 0, (len - curlen + 1)); /* also set trailing \0 byte */
    sdssetlen(s, len);
    return s;
}

/* Append the specified binary-safe string pointed by 't' of 'len' bytes to the
 * end of the specified sds string 's'.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatlen(sds s, const void *t, size_t len)
{
    size_t curlen = sdslen(s);

    s = sdsMakeRoomFor(s, len);
    if (s == NULL)
        return NULL;
    memcpy(s + curlen, t, len);
    sdssetlen(s, curlen + len);
    s[curlen + len] = '\0';
    return s;
}

/* Append the specified null termianted C string to the sds string 's'.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscat(sds s, const char *t)
{
    return sdscatlen(s, t, strlen(t));
}

/* Append the specified sds 't' to the existing sds 's'.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatsds(sds s, const sds t)
{
    return sdscatlen(s, t, sdslen(t));
}

/* Destructively modify the sds string 's' to hold the specified binary
 * safe string pointed by 't' of length 'len' bytes. */
sds sdscpylen(sds s, const char *t, size_t len)
{
    if (sdsalloc(s) < len)
    {
        // 如果原空间较小，重新申请
        s = sdsMakeRoomFor(s, len - sdslen(s));
        if (s == NULL)
            return NULL;
    }
    memcpy(s, t, len);
    s[len] = '\0';
    sdssetlen(s, len);
    return s;
}

/* Like sdscpylen() but 't' must be a null-termined string so that the length
 * of the string is obtained with strlen(). */
sds sdscpy(sds s, const char *t)
{
    return sdscpylen(s, t, strlen(t));
}

/* Helper for sdscatlonglong() doing the actual number -> string
 * conversion. 's' must point to a string with room for at least
 * SDS_LLSTR_SIZE bytes.
 *
 * The function returns the length of the null-terminated string
 * representation stored at 's'. */
#define SDS_LLSTR_SIZE 21
int sdsll2str(char *s, long long value)
{
    char *p, aux;
    unsigned long long v;
    size_t l;

    /* Generate the string representation, this method produces
     * an reversed string. */
    v = (value < 0) ? -value : value;
    p = s;
    do
    {
        *p++ = '0' + (v % 10);
        v /= 10;
    } while (v);
    if (value < 0)
        *p++ = '-';

    /* Compute length and add null term. */
    l = p - s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while (s < p)
    {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Identical sdsll2str(), but for unsigned long long type. */
int sdsull2str(char *s, unsigned long long v)
{
    char *p, aux;
    size_t l;

    /* Generate the string representation, this method produces
     * an reversed string. */
    p = s;
    do
    {
        *p++ = '0' + (v % 10);
        v /= 10;
    } while (v);

    /* Compute length and add null term. */
    l = p - s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while (s < p)
    {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Create an sds string from a long long value. It is much faster than:
 *
 * sdscatprintf(sdsempty(),"%lld\n", value);
 */
sds sdsfromlonglong(long long value)
{
    char buf[SDS_LLSTR_SIZE];
    int len = sdsll2str(buf, value);

    return sdsnewlen(buf, len);
}

/* Like sdscatprintf() but gets va_list instead of being variadic. */
sds sdscatvprintf(sds s, const char *fmt, va_list ap)
{
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf, *t;
    size_t buflen = strlen(fmt) * 2;

    /* We try to start using a static buffer for speed.
     * If not possible we revert to heap allocation. */
    if (buflen > sizeof(staticbuf))
    {
        buf = s_malloc(buflen);
        if (buf == NULL)
            return NULL;
    }
    else
    {
        buflen = sizeof(staticbuf);
    }

    // todo 通过多次循环的方式赋值，大部分应该是正常的，可以使用 len=vsnprintf(NULL, 0, fmt, cpy); 优化
    while (1)
    {
        buf[buflen - 2] = '\0';
        va_copy(cpy, ap);
        vsnprintf(buf, buflen, fmt, cpy);
        va_end(cpy);
        if (buf[buflen - 2] != '\0')
        {
            if (buf != staticbuf)
            {
                s_free(buf);
            }
            buflen *= 2;
            buf = s_malloc(buflen);
            if (buf == NULL)
                return NULL;
            continue;
        }
        break;
    }

    /* Finally concat the obtained string to the SDS string and return it. */
    t = sdscat(s, buf);
    if (buf != staticbuf)
        s_free(buf);
    return t;
}

/* Append to the sds string 's' a string obtained using printf-alike format
 * specifier.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("Sum is: ");
 * s = sdscatprintf(s,"%d+%d = %d",a,b,a+b).
 *
 * Often you need to create a string from scratch with the printf-alike
 * format. When this is the need, just use sdsempty() as the target string:
 *
 * s = sdscatprintf(sdsempty(), "... your format ...", args);
 */
sds sdscatprintf(sds s, const char *fmt, ...)
{
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s, fmt, ap);
    va_end(ap);
    return t;
}

/* This function is similar to sdscatprintf, but much faster as it does
 * not rely on sprintf() family functions implemented by the libc that
 * are often very slow. Moreover directly handling the sds string as
 * new data is concatenated provides a performance improvement.
 *
 * However this function only handles an incompatible subset of printf-alike
 * format specifiers:
 *
 * %s - C String
 * %S - SDS string
 * %i - signed int
 * %I - 64 bit signed integer (long long, int64_t)
 * %u - unsigned int
 * %U - 64 bit unsigned integer (unsigned long long, uint64_t)
 * %% - Verbatim "%" character.
 */
sds sdscatfmt(sds s, char const *fmt, ...)
{
    size_t initlen = sdslen(s);
    const char *f = fmt;
    long i;
    va_list ap;

    va_start(ap, fmt);
    f = fmt;     /* Next format specifier byte to process. */
    i = initlen; /* Position of the next byte to write to dest str. */
    while (*f)
    {
        char next, *str;
        size_t l;
        long long num;
        unsigned long long unum;

        /* Make sure there is always space for at least 1 char. */
        if (sdsavail(s) == 0)
        {
            s = sdsMakeRoomFor(s, 1);
        }

        switch (*f)
        {
        case '%':
            next = *(f + 1);
            f++;
            switch (next)
            {
            case 's':
            case 'S':
                str = va_arg(ap, char *);
                l = (next == 's') ? strlen(str) : sdslen(str);
                if (sdsavail(s) < l)
                {
                    s = sdsMakeRoomFor(s, l);
                }
                memcpy(s + i, str, l);
                sdsinclen(s, l);
                i += l;
                break;
            case 'i':
            case 'I':
                if (next == 'i')
                    num = va_arg(ap, int);
                else
                    num = va_arg(ap, long long);
                {
                    char buf[SDS_LLSTR_SIZE];
                    l = sdsll2str(buf, num);
                    if (sdsavail(s) < l)
                    {
                        s = sdsMakeRoomFor(s, l);
                    }
                    memcpy(s + i, buf, l);
                    sdsinclen(s, l);
                    i += l;
                }
                break;
            case 'u':
            case 'U':
                if (next == 'u')
                    unum = va_arg(ap, unsigned int);
                else
                    unum = va_arg(ap, unsigned long long);
                {
                    char buf[SDS_LLSTR_SIZE];
                    l = sdsull2str(buf, unum);
                    if (sdsavail(s) < l)
                    {
                        s = sdsMakeRoomFor(s, l);
                    }
                    memcpy(s + i, buf, l);
                    sdsinclen(s, l);
                    i += l;
                }
                break;
            default: /* Handle %% and generally %<unknown>. */
                s[i++] = next;
                sdsinclen(s, 1);
                break;
            }
            break;
        default:
            s[i++] = *f;
            sdsinclen(s, 1);
            break;
        }
        f++;
    }
    va_end(ap);

    /* Add null-term */
    s[i] = '\0';
    return s;
}

/*
 * 删除s头和尾包含cset内字符
 * Example:
 *
 * s = sdsnew("AA...AA.a.aa.aHelloWorld     :::");
 * s = sdstrim(s,"Aa. :");
 * printf("%s\n", s);
 *
 * Output will be just "HelloWorld".
 *
 */
sds sdstrim(sds s, const char *cset)
{
    char *start, *end, *sp, *ep;
    size_t len;

    sp = start = s;
    ep = end = s + sdslen(s) - 1; // -1是去掉'\0'
    while (sp <= end && strchr(cset, *sp))
        sp++;
    while (ep > sp && strchr(cset, *ep))
        ep--;
    len = (sp > ep) ? 0 : ((ep - sp) + 1);
    if (s != sp)
    {
        memmove(s, sp, len);
    }
    s[len] = '\0';
    sdssetlen(s, len);
    return s;
}

/* Turn the string into a smaller (or equal) string containing only the
 * substring specified by the 'start' and 'end' indexes.
 *
 * start and end can be negative, where -1 means the last character of the
 * string, -2 the penultimate character, and so forth.
 *
 * The interval is inclusive, so the start and end characters will be part
 * of the resulting string.
 *
 * The string is modified in-place.
 *
 * Example:
 *
 * s = sdsnew("Hello World");
 * sdsrange(s,1,-1); => "ello World"
 */
void sdsrange(sds s, ssize_t start, ssize_t end)
{
    size_t newlen, len = sdslen(s);

    if (len == 0)
    {
        return;
    }
    if (start < 0)
    {
        start = len + start;
        if (start < 0)
        { // start 设置的过小
            start = 0;
        }
    }
    if (end < 0)
    {
        end = len + end;
        if (end < 0)
        { // end 设置的过小
            end = 0;
        }
    }
    newlen = (start > end) ? 0 : (end - start) + 1;
    if (newlen != 0)
    {
        if (start >= (ssize_t)len)
        { // start过大
            newlen = 0;
        }
        else if (end >= (ssize_t)len)
        {
            end = len - 1; // end过大直接置为末尾
            newlen = (start > end) ? 0 : (end - start) + 1;
        }
    }
    else
    { // newlen==0
        start = 0;
    }
    if (start && newlen)
    {
        memmove(s, s + start, newlen);
    }
    s[newlen] = 0;
    sdssetlen(s, newlen);
}

/* Apply tolower() to every character of the sds string 's'. */
void sdstolower(sds s)
{
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++)
        s[j] = tolower(s[j]);
}

/* Apply toupper() to every character of the sds string 's'. */
void sdstoupper(sds s)
{
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++)
        s[j] = toupper(s[j]);
}

/* Compare two sds strings s1 and s2 with memcmp().
 *
 * Return value:
 *
 *     positive if s1 > s2.
 *     negative if s1 < s2.
 *     0 if s1 and s2 are exactly the same binary string.
 *
 * If two strings share exactly the same prefix, but one of the two has
 * additional characters, the longer string is considered to be greater than
 * the smaller one. */
int sdscmp(const sds s1, const sds s2)
{
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1, s2, minlen);
    if (cmp == 0)
    {
        return l1 > l2 ? 1 : (l1 < l2 ? -1 : 0);
    }
    return cmp;
}

/* Split 's' with separator in 'sep'. An array
 * of sds strings is returned. *count will be set
 * by reference to the number of tokens returned.
 *
 * On out of memory, zero length string, zero length
 * separator, NULL is returned.
 *
 * Note that 'sep' is able to split a string using
 * a multi-character separator. For example
 * sdssplit("foo_-_bar","_-_"); will return two
 * elements "foo" and "bar".
 *
 * This version of the function is binary-safe but
 * requires length arguments. sdssplit() is just the
 * same function but for zero-terminated strings.
 */
sds *sdssplitlen(const char *s, ssize_t len, const char *sep, int seplen, int *count)
{
    int elements = 0, slots = 5;
    long start = 0, j;
    sds *tokens;

    if (seplen < 1 || len < 0)
        return NULL;

    tokens = s_malloc(sizeof(sds) * slots);
    if (tokens == NULL)
        return NULL;

    if (len == 0)
    {
        *count = 0;
        return tokens;
    }
    for (j = 0; j < (len - (seplen - 1)); j++)
    {
        /* make sure there is room for the next element and the final one */
        if (slots < elements + 2)
        {
            sds *newtokens;

            slots *= 2;
            newtokens = s_realloc(tokens, sizeof(sds) * slots);
            if (newtokens == NULL)
                goto cleanup;
            tokens = newtokens;
        }
        /* search the separator */
        if ((seplen == 1 && *(s + j) == sep[0]) || (memcmp(s + j, sep, seplen) == 0))
        {
            tokens[elements] = sdsnewlen(s + start, j - start);
            if (tokens[elements] == NULL)
                goto cleanup;
            elements++;
            start = j + seplen;
            j = j + seplen - 1; /* skip the separator */
        }
    }
    /* Add the final element. We are sure there is room in the tokens array. */
    tokens[elements] = sdsnewlen(s + start, len - start);
    if (tokens[elements] == NULL)
        goto cleanup;
    elements++;
    *count = elements;
    return tokens;

cleanup:
{
    int i;
    for (i = 0; i < elements; i++)
        sdsfree(tokens[i]);
    s_free(tokens);
    *count = 0;
    return NULL;
}
}

/* Free the result returned by sdssplitlen(), or do nothing if 'tokens' is NULL. */
void sdsfreesplitres(sds *tokens, int count)
{
    if (!tokens)
        return;
    while (count--)
        sdsfree(tokens[count]);
    s_free(tokens);
}

/* p指向的字符串里不可打印的字符转为\x<十六进制>
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatrepr(sds s, const char *p, size_t len)
{
    s = sdscatlen(s, "\"", 1);
    while (len--)
    {
        switch (*p)
        {
        case '\\':
        case '"':
            s = sdscatprintf(s, "\\%c", *p);
            break;
        case '\n':
            s = sdscatlen(s, "\\n", 2);
            break;
        case '\r':
            s = sdscatlen(s, "\\r", 2);
            break;
        case '\t':
            s = sdscatlen(s, "\\t", 2);
            break;
        case '\a':
            s = sdscatlen(s, "\\a", 2);
            break;
        case '\b':
            s = sdscatlen(s, "\\b", 2);
            break;
        default:
            if (isprint(*p))
                s = sdscatprintf(s, "%c", *p);
            else
                s = sdscatprintf(s, "\\x%02x", (unsigned char)*p);
            break;
        }
        p++;
    }
    return sdscatlen(s, "\"", 1);
}

/* Helper function for sdssplitargs() that returns non zero if 'c'
 * is a valid hex digit. */
int is_hex_digit(char c)
{
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* Helper function for sdssplitargs() that converts a hex digit into an
 * integer from 0 to 15 */
int hex_digit_to_int(char c)
{
    switch (c)
    {
    case '0':
        return 0;
    case '1':
        return 1;
    case '2':
        return 2;
    case '3':
        return 3;
    case '4':
        return 4;
    case '5':
        return 5;
    case '6':
        return 6;
    case '7':
        return 7;
    case '8':
        return 8;
    case '9':
        return 9;
    case 'a':
    case 'A':
        return 10;
    case 'b':
    case 'B':
        return 11;
    case 'c':
    case 'C':
        return 12;
    case 'd':
    case 'D':
        return 13;
    case 'e':
    case 'E':
        return 14;
    case 'f':
    case 'F':
        return 15;
    default:
        return 0;
    }
}

/* Split a line into arguments, where every argument can be in the
 * following programming-language REPL-alike form:
 *
 * foo bar "newline are supported\n" and "\xff\x00otherstuff"
 *
 * The number of arguments is stored into *argc, and an array
 * of sds is returned.
 *
 * The caller should free the resulting array of sds strings with
 * sdsfreesplitres().
 *
 * Note that sdscatrepr() is able to convert back a string into
 * a quoted string in the same format sdssplitargs() is able to parse.
 *
 * The function returns the allocated tokens on success, even when the
 * input string is empty, or NULL if the input contains unbalanced
 * quotes or closed quotes followed by non space characters
 * as in: "foo"bar or "foo'
 */
sds *sdssplitargs(const char *line, int *argc)
{
    const char *p = line;
    char *current = NULL;
    char **vector = NULL;

    *argc = 0;
    while (1)
    {
        /* skip blanks */
        while (*p && isspace(*p))
            p++;
        if (*p)
        {
            /* get a token */
            int inq = 0;  /* set to 1 if we are in "quotes" */
            int insq = 0; /* set to 1 if we are in 'single quotes' */
            int done = 0;

            if (current == NULL)
                current = sdsempty();
            while (!done)
            {
                if (inq)
                {
                    if (*p == '\\' && *(p + 1) == 'x' &&
                        is_hex_digit(*(p + 2)) &&
                        is_hex_digit(*(p + 3)))
                    {
                        unsigned char byte;

                        byte = (hex_digit_to_int(*(p + 2)) * 16) +
                               hex_digit_to_int(*(p + 3));
                        current = sdscatlen(current, (char *)&byte, 1);
                        p += 3;
                    }
                    else if (*p == '\\' && *(p + 1))
                    {
                        char c;

                        p++;
                        switch (*p)
                        {
                        case 'n':
                            c = '\n';
                            break;
                        case 'r':
                            c = '\r';
                            break;
                        case 't':
                            c = '\t';
                            break;
                        case 'b':
                            c = '\b';
                            break;
                        case 'a':
                            c = '\a';
                            break;
                        default:
                            c = *p;
                            break;
                        }
                        current = sdscatlen(current, &c, 1);
                    }
                    else if (*p == '"')
                    {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1)))
                            goto err;
                        done = 1;
                    }
                    else if (!*p)
                    {
                        /* unterminated quotes */
                        goto err;
                    }
                    else
                    {
                        current = sdscatlen(current, p, 1);
                    }
                }
                else if (insq)
                {
                    if (*p == '\\' && *(p + 1) == '\'')
                    {
                        p++;
                        current = sdscatlen(current, "'", 1);
                    }
                    else if (*p == '\'')
                    {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1)))
                            goto err;
                        done = 1;
                    }
                    else if (!*p)
                    {
                        /* unterminated quotes */
                        goto err;
                    }
                    else
                    {
                        current = sdscatlen(current, p, 1);
                    }
                }
                else
                {
                    switch (*p)
                    {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                        done = 1;
                        break;
                    case '"':
                        inq = 1;
                        break;
                    case '\'':
                        insq = 1;
                        break;
                    default:
                        current = sdscatlen(current, p, 1);
                        break;
                    }
                }
                if (*p)
                    p++;
            }
            /* add the token to the vector */
            vector = s_realloc(vector, ((*argc) + 1) * sizeof(char *));
            vector[*argc] = current;
            (*argc)++;
            current = NULL;
        }
        else
        {
            /* Even on empty input string return something not NULL. */
            if (vector == NULL)
                vector = s_malloc(sizeof(void *));
            return vector;
        }
    }

err:
    while ((*argc)--)
        sdsfree(vector[*argc]);
    s_free(vector);
    if (current)
        sdsfree(current);
    *argc = 0;
    return NULL;
}

/* Modify the string substituting all the occurrences of the set of
 * characters specified in the 'from' string to the corresponding character
 * in the 'to' array.
 *
 * For instance: sdsmapchars(mystring, "ho", "01", 2)
 * will have the effect of turning the string "hello" into "0ell1".
 *
 * The function returns the sds string pointer, that is always the same
 * as the input pointer since no resize is needed. */
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen)
{
    size_t j, i, l = sdslen(s);

    for (j = 0; j < l; j++)
    {
        for (i = 0; i < setlen; i++)
        {
            if (s[j] == from[i])
            {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

/* Join an array of C strings using the specified separator (also a C string).
 * Returns the result as an sds string. */
sds sdsjoin(char **argv, int argc, char *sep)
{
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++)
    {
        join = sdscat(join, argv[j]);
        if (j != argc - 1)
            join = sdscat(join, sep);
    }
    return join;
}

/* Like sdsjoin, but joins an array of SDS strings. */
sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen)
{
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++)
    {
        join = sdscatsds(join, argv[j]);
        if (j != argc - 1)
            join = sdscatlen(join, sep, seplen);
    }
    return join;
}

/* Wrappers to the allocators used by SDS. Note that SDS will actually
 * just use the macros defined into sdsalloc.h in order to avoid to pay
 * the overhead of function calls. Here we define these wrappers only for
 * the programs SDS is linked to, if they want to touch the SDS internals
 * even if they use a different allocator. */
void *sds_malloc(size_t size) { return s_malloc(size); }
void *sds_realloc(void *ptr, size_t size) { return s_realloc(ptr, size); }
void sds_free(void *ptr) { s_free(ptr); }

// sds单元测试用例
// #if defined(SDS_TEST_MAIN)
#include <stdio.h>
#include "testhelp.h"
#include "limits.h"

#define UNUSED(x) (void)(x)
int sdsTest(void)
{
    {
        sds x = sdsnew("foo"), y; // x="foo"

        test_cond("Create a string and obtain the length",
                  sdslen(x) == 3 && memcmp(x, "foo\0", 4) == 0);
        sdsfree(x); // unuse x

        x = sdsnewlen("foo", 2); // x="fo"
        test_cond("Create a string with specified length",
                  sdslen(x) == 2 && memcmp(x, "fo\0", 3) == 0);

        x = sdscat(x, "bar"); // x="fobar"
        test_cond("Strings concatenation",
                  sdslen(x) == 5 && memcmp(x, "fobar\0", 6) == 0);

        x = sdscpy(x, "a"); // x="a"
        test_cond("sdscpy() against an originally longer string",
                  sdslen(x) == 1 && memcmp(x, "a\0", 2) == 0);

        x = sdscpy(x, "xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk"); // x="xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk" 33个字符
        test_cond("sdscpy() against an originally shorter string",
                  sdslen(x) == 33 &&
                      memcmp(x, "xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk\0", 33) == 0)

            sdsfree(x);                          // unuse x
        x = sdscatprintf(sdsempty(), "%d", 123); // x=123
        test_cond("sdscatprintf() seems working in the base case",
                  sdslen(x) == 3 && memcmp(x, "123\0", 4) == 0)

            sdsfree(x);
        x = sdsnew("--"); // x=--
        x = sdscatfmt(x, "Hello %s World %I,%I--", "Hi!", LLONG_MIN, LLONG_MAX);
        test_cond("sdscatfmt() seems working in the base case",
                  sdslen(x) == 60 &&
                      memcmp(x, "--Hello Hi! World -9223372036854775808,"
                                "9223372036854775807--",
                             60) == 0)
            printf("[%s]\n", x); // x=up string

        sdsfree(x);
        x = sdsnew("--");
        x = sdscatfmt(x, "%u,%U--", UINT_MAX, ULLONG_MAX);
        test_cond("sdscatfmt() seems working with unsigned numbers",
                  sdslen(x) == 35 &&
                      memcmp(x, "--4294967295,18446744073709551615--", 35) == 0)

            sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x, " x"); // x="x"
        test_cond("sdstrim() works when all chars match",
                  sdslen(x) == 0)

            sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x, " "); // ok
        test_cond("sdstrim() works when a single char remains",
                  sdslen(x) == 1 && x[0] == 'x')

            sdsfree(x);
        x = sdsnew("xxciaoyyy");
        sdstrim(x, "xy"); // ok
        test_cond("sdstrim() correctly trims characters",
                  sdslen(x) == 4 && memcmp(x, "ciao\0", 5) == 0);

        y = sdsdup(x);     // y=ciao
        sdsrange(y, 1, 1); // y="i"
        test_cond("sdsrange(...,1,1)",
                  sdslen(y) == 1 && memcmp(y, "i\0", 2) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y, 1, -1); // y="iao"
        test_cond("sdsrange(...,1,-1)",
                  sdslen(y) == 3 && memcmp(y, "iao\0", 4) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y, -2, -1); // y="ao"
        test_cond("sdsrange(...,-2,-1)",
                  sdslen(y) == 2 && memcmp(y, "ao\0", 3) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y, 2, 1); // y=""
        test_cond("sdsrange(...,2,1)",
                  sdslen(y) == 0 && memcmp(y, "\0", 1) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y, 1, 100); // y = "iao"
        test_cond("sdsrange(...,1,100)",
                  sdslen(y) == 3 && memcmp(y, "iao\0", 4) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y, 100, 100); // y=""
        test_cond("sdsrange(...,100,100)",
                  sdslen(y) == 0 && memcmp(y, "\0", 1) == 0)

            sdsfree(y);
        sdsfree(x);
        x = sdsnew("foo");
        y = sdsnew("foa");
        test_cond("sdscmp(foo,foa)", sdscmp(x, y) > 0) // ok

            sdsfree(y);
        sdsfree(x);
        x = sdsnew("bar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x, y) == 0) // ok

            sdsfree(y);
        sdsfree(x);
        x = sdsnew("aar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x, y) < 0); // ok

        sdsfree(y);
        sdsfree(x);
        x = sdsnewlen("\a\n\0foo\r", 7);
        y = sdscatrepr(sdsempty(), x, sdslen(x));
        test_cond("sdscatrepr(...data...)",
                  memcmp(y, "\"\\a\\n\\x00foo\\r\"", 15) == 0)

        {
            unsigned int oldfree;
            char *p;
            int step = 10, j, i;

            sdsfree(x);
            sdsfree(y);
            x = sdsnew("0");
            test_cond("sdsnew() free/len buffers", sdslen(x) == 1 && sdsavail(x) == 0);

            /* Run the test a few times in order to hit the first two
             * SDS header types. */
            for (i = 0; i < 10; i++)
            {
                int oldlen = sdslen(x);
                x = sdsMakeRoomFor(x, step);
                int type = x[-1] & SDS_TYPE_MASK;

                test_cond("sdsMakeRoomFor() len", sdslen(x) == oldlen);
                if (type != SDS_TYPE_5)
                {
                    test_cond("sdsMakeRoomFor() free", sdsavail(x) >= step);
                    oldfree = sdsavail(x);
                }
                p = x + oldlen;
                for (j = 0; j < step; j++)
                {
                    p[j] = 'A' + j;
                }
                sdsIncrLen(x, step);
            }
            test_cond("sdsMakeRoomFor() content",
                      memcmp("0ABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJ", x, 101) == 0);
            test_cond("sdsMakeRoomFor() final length", sdslen(x) == 101);

            sdsfree(x);
        }
    }
    test_report();
    return 0;
}
// #endif

// 执行单元测试 gcc -g -o sdsmain sds.c zmalloc.c -DSDS_TEST_MAIN
#ifdef SDS_TEST_MAIN
int main(void)
{
    return sdsTest();
}
#endif
