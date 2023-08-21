/*
    Copyright 2023 Safayet N Ahmed

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    1.  Redistributions of source code must retain the above copyright notice,
        this list of conditions and the following disclaimer.

    2.  Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.

    3.  Neither the name of the copyright holder nor the names of its
        contributors may be used to endorse or promote products derived from
        this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS “AS IS”
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
    ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
    LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
    CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
    SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
    INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
    ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef __RBA_H__
#define __RBA_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define RBA_ERR(...) fprintf(stderr, "RBA_ERROR: " __FILE__  "(" TOSTRING(__LINE__) "): " __VA_ARGS__ )

#define RBA_ERRNO() fprintf(stderr, "RBA_ERROR: " __FILE__  "(" TOSTRING(__LINE__) ") [%s]: %s\n", __func__, strerror(errno))


#define for_each_csvtoken(line, iterator, tok) for(iterator=line, tok=strsep(&iterator, ","); tok != NULL; tok=strsep(&iterator, ","))

extern char*
rba_strtrim (char *string);

extern int
strtoint64 (const char  *str,
            int64_t     *out_p);

extern int
strtouint64 (   const char  *str,
                uint64_t    *out_p);

extern int
strtodouble (   const char  *str,
                double      *out_p);

#define RBA_HEADER_MAGIC 0x52414E4942574152 /* RAWBINAR */
#ifndef RBA_HEADER_VERSION
    #define RBA_HEADER_VERSION 0x0000
#endif

typedef struct {
    uint64_t    rba_header_magic;
    uint64_t    rba_type_magic;
    uint64_t    records;
    uint32_t    data_offset;
    uint16_t    typesize;
    uint16_t    rba_header_version;
} rba_header_t;

struct rba_type_s;
typedef struct rba_type_s rba_type_t;

typedef struct {
    FILE        *filep;
    void        *arr;
    uint64_t    total;
    size_t      elm_sz;
    size_t      len;
    size_t      idx;
} rba_buf_t;

#define RBA_BUF_DEFAULTLEN (4096)

extern int
rba_buf_alloc ( rba_type_t  *type,
                const char  *filename,
                rba_buf_t   *buf);

extern int
rba_buf_simple_flush (rba_buf_t *buf);

extern int
rba_buf_simple_free (   rba_type_t  *type,
                        rba_buf_t   *buf);

extern rba_type_t rba_type_ignore;
extern rba_type_t rba_type_u8;
extern rba_type_t rba_type_i8;
extern rba_type_t rba_type_u16;
extern rba_type_t rba_type_i16;
extern rba_type_t rba_type_u32;
extern rba_type_t rba_type_i32;
extern rba_type_t rba_type_u64;
extern rba_type_t rba_type_i64;
extern rba_type_t rba_type_float;
extern rba_type_t rba_type_double;

typedef struct {
    const char      *name;
    rba_type_t      *type;
} rba_spec_entry_t;

typedef struct {
    rba_spec_entry_t    *spec;
    rba_buf_t           *bufs;
    uint32_t            *partsmpl_remaining;
    uint32_t            *partidxbuf;
    uint64_t            totsmpl_remaining;
    uint32_t            cols;
    uint32_t            partitions;
    uint32_t            repetitions;
    uint64_t            rng_state;
} rba_data_t;

extern int
rba_checkhdr_countrecords ( rba_spec_entry_t    *spec,
                            uint32_t            cols,
                            const char          *filename,
                            uint64_t            *reccount_p);

extern int
rba_data_alloc (rba_data_t          *data,
                rba_spec_entry_t    *spec,
                uint32_t            cols,
                const char          *dirpath,
                uint32_t            partitions,
                uint32_t            repetitions,
                uint64_t            samples);

extern int
rba_data_free (rba_data_t *data);

#define rba_data_getcolbufs(data, col) (&(data->bufs[col * data->partitions]))

extern int
rba_data_parse_line (   rba_data_t  *data,
                        char        *nextline);

extern int
rba_data_parse_csvs (   rba_data_t  *data,
                        const char  **csvnames,
                        int         csvcount);

typedef int (*rba_type_initbuf_t) ( rba_type_t  *type,
                                    const char  *filename,
                                    rba_buf_t   *buf);

typedef int (*rba_type_freebuf_t) ( rba_type_t  *type,
                                    rba_buf_t   *buf);

typedef int (*rba_type_parse_t) (   rba_data_t  *data,
                                    uint32_t    col,
                                    const char  *string);

struct rba_type_s {
    const char*         specname;
    uint64_t            magic;
    size_t              size;
    rba_type_initbuf_t  initbuf;
    rba_type_freebuf_t  freebuf;
    rba_type_parse_t    parse;
};

#endif /* #ifndef __RBA_H__ __RBA_H__ */
