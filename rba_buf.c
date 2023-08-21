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

#include <stddef.h>

#include <rba.h>

int
rba_buf_alloc ( rba_type_t  *type,
                const char  *filename,
                rba_buf_t   *buf)
{
    int ret;

    FILE *filep;

    size_t elm_sz, len;
    void *arr;

    rba_header_t hdr;

    filep = fopen (filename, "wb");
    if (NULL == filep) {
        RBA_ERR("Failed to open file %s\n", filename);
        RBA_ERRNO();
        ret = -1;
    } else {

        elm_sz = type->size;
        len = RBA_BUF_DEFAULTLEN;
        arr = malloc(elm_sz * RBA_BUF_DEFAULTLEN);
        if (NULL == arr) {
            RBA_ERR("Failed to malloc buffer for type %s\n", type->specname);
            ret = -1;
        } else {

            hdr.rba_header_magic   = RBA_HEADER_MAGIC;
            hdr.rba_type_magic     = type->magic;
            hdr.records            = 0;
            hdr.data_offset        = sizeof(rba_header_t);
            hdr.typesize           = type->size;
            hdr.rba_header_version = RBA_HEADER_VERSION;
            if (1 != fwrite (   &hdr,
                                sizeof(rba_header_t),
                                1,
                                filep)) {
                RBA_ERR("Failed to write header (%p) to file %s\n", (void*)&hdr, filename);
                ret = -1;
            } else {

                buf->filep = filep;
                buf->arr = arr;
                buf->total = 0;
                buf->elm_sz = elm_sz;
                buf->len = len;
                buf->idx = 0;
                ret = 0;
            }

            if (0 != ret) {
                free (arr);
            }
        }

        if (0 != ret) {
            fclose(filep);
        }
    }

    return ret;
}

int
rba_buf_simple_flush (rba_buf_t *buf)
{
    int ret;

    if (buf->idx == 0) {
        ret = 0;
    } else {
        size_t write_sz = buf->idx * buf->elm_sz;
        if (1 != fwrite(buf->arr,
                        write_sz,
                        1,
                        buf->filep)) {
            RBA_ERR("failed to write %li bytes at %p\n", write_sz, (void*)buf->arr);
            ret = -1;
        } else {
            buf->total += buf->idx;
            buf->idx = 0;
            ret = 0;
        }
    }

    return ret;
}

int
rba_buf_simple_free (   rba_type_t  *type,
                        rba_buf_t   *buf)
{
    int ret;

    size_t offset;

    /*  write out any existing data */
    ret = rba_buf_simple_flush (buf);
    if (0 != ret) {
        RBA_ERR("rba_buf_simple_flush failed for %s rba_buf_t\n", type->specname);
        ret = -1;
    } else {

        /*  fix up the header */
        offset = offsetof(rba_header_t, records);
        ret = fseek (   buf->filep,
                        offset,
                        SEEK_SET);
        if (0 != ret) {
            RBA_ERRNO();
            ret = -1;
        } else {

            if (1 != fwrite (   &(buf->total),
                                sizeof(buf->total),
                                1,
                                buf->filep)) {
                RBA_ERR("failed to write %li bytes at %p\n", sizeof(buf->total), (void*)&(buf->total));
                ret = -1;
            } else {
                RBA_ERR("Closing file\n");
                if (0 != fclose(buf->filep)) {
                    RBA_ERRNO();
                    ret = -1;
                } else {

                    free(buf->arr);
                    memset(buf, 0, sizeof(rba_buf_t));
                    ret = 0;
                }
            }
        }
    }

    return ret;
}

