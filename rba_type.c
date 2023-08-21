#include <math.h>
#include <stdlib.h>
#include <rba.h>

/******************************************************************************/
/*  basic string parsing functions                                            */
/******************************************************************************/

int
strtoint64 (const char  *str,
            int64_t     *out_p)
{
    int ret;
    int64_t conv;
    char *endptr;

    errno = 0;
    conv = (int64_t) strtoll (str, &endptr, 0);
    if (0 != errno) {
        RBA_ERR("failed to convert %s to int64_t\n", str);
        RBA_ERRNO();
        ret = -1;
    } else {
        if (endptr == str) {
            RBA_ERR("failed to convert %s to int64_t\n", str);
            errno = EINVAL;
            ret = -1;
        } else {
            *out_p = conv;
            ret = 0;
        }
    }

    return ret;
}

int
strtouint64 (   const char  *str,
                uint64_t    *out_p)
{
    int ret;
    uint64_t conv;
    char *endptr;

    errno = 0;
    conv = (uint64_t) strtoull (str, &endptr, 0);
    if (0 != errno) {
        RBA_ERR("failed to convert %s to uint64_t\n", str);
        RBA_ERRNO();
        ret = -1;
    } else {
        if (endptr == str) {
            RBA_ERR("failed to convert %s to uint64_t\n", str);
            errno = EINVAL;
            ret = -1;
        } else {
            *out_p = conv;
            ret = 0;
        }
    }

    return ret;
}

int
strtodouble (   const char  *str,
                double      *out_p)
{
    int ret;
    double conv;
    char *endptr;

    errno = 0;
    conv = (float) strtod (str, &endptr);
    if (0 != errno) {        
        RBA_ERR("failed to convert %s to double\n", str);
        RBA_ERRNO();
        ret = -1;
    } else {
        if (endptr == str) {
            RBA_ERR("missing value: %s\n", str);
            errno = EINVAL;
            ret = -1;
        } else {
            *out_p = conv;
            ret = 0;
        }
    }

    return ret;
}

/******************************************************************************/
/*  rba_type_ignore_ functions                                                */
/******************************************************************************/

int
rba_type_ignore_initbuf (   rba_type_t  *type,
                            const char  *filename,
                            rba_buf_t   *buf)
{
    (void)type;
    (void)filename;
    memset (buf, 0, sizeof(rba_buf_t));
    return 0;
}

int
rba_type_ignore_freebuf (   rba_type_t  *type,
                            rba_buf_t   *buf)
{
    (void)type;
    (void)buf;
    return 0;
}

int
rba_type_ignore_parse ( rba_data_t  *data,
                        uint32_t    col,
                        const char  *string)
{
    (void)data;
    (void)col;
    (void)string;
    return 0;
}

/******************************************************************************/
/*  rba_type_..._parse functions                                              */
/******************************************************************************/

int
rba_type_u8_parse ( rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    uint64_t val64;

    ret = strtouint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to uint64\n", string);
    } else {
        if (val64 > UINT8_MAX) {
            RBA_ERR("integer %s is out of range for uint8\n", string);
            errno = ERANGE;
            ret = -1;
        } else {
            uint32_t    r, p;
            rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

            for (r=0, ret=0; \
                    (r < data->repetitions) && (0 == ret) ;
                        r++) {

                p = data->partidxbuf[r];
                
                ((uint8_t*)(bufs[p].arr))[bufs[p].idx] = (uint8_t)val64;
                bufs[p].idx++;
                if (bufs[p].idx == bufs[p].len) {

                    ret = rba_buf_simple_flush (&(bufs[p]));
                    if (0 != ret) {
                        RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                        ret = -1;
                    }
                }
            }
        }
    }

    return ret;
}

int
rba_type_i8_parse ( rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    int64_t val64;

    ret = strtoint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to int64\n", string);
    } else {
        if ((val64 > INT8_MAX) || (val64 < INT8_MIN)) {
            RBA_ERR("integer %s is out of range for int8\n", string);
            errno = ERANGE;
            ret = -1;
        } else {
            uint32_t    r, p;
            rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

            for (r=0, ret=0; \
                    (r < data->repetitions) && (0 == ret) ;
                        r++) {

                p = data->partidxbuf[r];
                
                ((int8_t*)(bufs[p].arr))[bufs[p].idx] = (int8_t)val64;
                bufs[p].idx++;
                if (bufs[p].idx == bufs[p].len) {

                    ret = rba_buf_simple_flush (&(bufs[p]));
                    if (0 != ret) {
                        RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                        ret = -1;
                    }
                }
            }
        }
    }

    return ret;
}

int
rba_type_u16_parse (rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    uint64_t val64;

    ret = strtouint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to uint64\n", string);
    } else {
        if (val64 > UINT16_MAX) {
            RBA_ERR("integer %s is out of range for uint16\n", string);
            errno = ERANGE;
            ret = -1;
        } else {
            uint32_t    r, p;
            rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

            for (r=0, ret=0; \
                    (r < data->repetitions) && (0 == ret) ;
                        r++) {

                p = data->partidxbuf[r];
                
                ((uint16_t*)(bufs[p].arr))[bufs[p].idx] = (uint16_t)val64;
                bufs[p].idx++;
                if (bufs[p].idx == bufs[p].len) {

                    ret = rba_buf_simple_flush (&(bufs[p]));
                    if (0 != ret) {
                        RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                        ret = -1;
                    }
                }
            }
        }
    }

    return ret;
}

int
rba_type_i16_parse (rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    int64_t val64;

    ret = strtoint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to int64\n", string);
    } else {
        if ((val64 > INT16_MAX) || (val64 < INT16_MIN)) {
            RBA_ERR("integer %s is out of range for int16\n", string);
            errno = ERANGE;
            ret = -1;
        } else {
            uint32_t    r, p;
            rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

            for (r=0, ret=0; \
                    (r < data->repetitions) && (0 == ret) ;
                        r++) {

                p = data->partidxbuf[r];
                
                ((int16_t*)(bufs[p].arr))[bufs[p].idx] = (int16_t)val64;
                bufs[p].idx++;
                if (bufs[p].idx == bufs[p].len) {

                    ret = rba_buf_simple_flush (&(bufs[p]));
                    if (0 != ret) {
                        RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                        ret = -1;
                    }
                }
            }
        }
    }

    return ret;
}

int
rba_type_u32_parse (rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    uint64_t val64;

    ret = strtouint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to uint64\n", string);
    } else {
        if (val64 > UINT32_MAX) {
            RBA_ERR("integer %s is out of range for uint32\n", string);
            errno = ERANGE;
            ret = -1;
        } else {
            uint32_t    r, p;
            rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

            for (r=0, ret=0; \
                    (r < data->repetitions) && (0 == ret) ;
                        r++) {

                p = data->partidxbuf[r];
                
                ((uint32_t*)(bufs[p].arr))[bufs[p].idx] = (uint32_t)val64;
                bufs[p].idx++;
                if (bufs[p].idx == bufs[p].len) {

                    ret = rba_buf_simple_flush (&(bufs[p]));
                    if (0 != ret) {
                        RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                        ret = -1;
                    }
                }
            }
        }
    }

    return ret;
}

int
rba_type_i32_parse (rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    int64_t val64;

    ret = strtoint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to int64\n", string);
    } else {
        if ((val64 > INT32_MAX) || (val64 < INT32_MIN)) {
            RBA_ERR("integer %s is out of range for int32\n", string);
            errno = ERANGE;
            ret = -1;
        } else {
            uint32_t    r, p;
            rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

            for (r=0, ret=0; \
                    (r < data->repetitions) && (0 == ret) ;
                        r++) {

                p = data->partidxbuf[r];
                
                ((int32_t*)(bufs[p].arr))[bufs[p].idx] = (int32_t)val64;
                bufs[p].idx++;
                if (bufs[p].idx == bufs[p].len) {

                    ret = rba_buf_simple_flush (&(bufs[p]));
                    if (0 != ret) {
                        RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                        ret = -1;
                    }
                }
            }
        }
    }

    return ret;
}

int
rba_type_u64_parse (rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    uint64_t val64;

    ret = strtouint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to uint64\n", string);
    } else {
        uint32_t    r, p;
        rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

        for (r=0, ret=0; \
                (r < data->repetitions) && (0 == ret) ;
                    r++) {

            p = data->partidxbuf[r];
            
            ((uint64_t*)(bufs[p].arr))[bufs[p].idx] = val64;
            bufs[p].idx++;
            if (bufs[p].idx == bufs[p].len) {

                ret = rba_buf_simple_flush (&(bufs[p]));
                if (0 != ret) {
                    RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                    ret = -1;
                }
            }
        }
    }

    return ret;
}

int
rba_type_i64_parse (rba_data_t  *data,
                    uint32_t    col,
                    const char  *string)
{
    int ret;
    int64_t val64;

    ret = strtoint64 (string, &val64);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to int64\n", string);
    } else {
        uint32_t    r, p;
        rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

        for (r=0, ret=0; \
                (r < data->repetitions) && (0 == ret) ;
                    r++) {

            p = data->partidxbuf[r];
            
            ((int64_t*)(bufs[p].arr))[bufs[p].idx] = val64;
            bufs[p].idx++;
            if (bufs[p].idx == bufs[p].len) {

                ret = rba_buf_simple_flush (&(bufs[p]));
                if (0 != ret) {
                    RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                    ret = -1;
                }
            }
        }
    }

    return ret;
}

int
rba_type_float_parse (  rba_data_t  *data,
                        uint32_t    col,
                        const char  *string)
{
    int ret;
    double valdbl;

    uint32_t    r, p;

    rba_buf_t *bufs;

    ret = strtodouble (string, &valdbl);
    if (-1 == ret) {
        /*RBA_ERR("failed to convert %s to double\n", string);
        RBA_ERR("Replacing missing value in column %u (%s) with 0\n", (unsigned) col, data->spec[col].name);*/
        valdbl = 0;
        ret = 0;
    } 
        
    bufs = rba_data_getcolbufs(data, col);

    for (r=0, ret=0; \
            (r < data->repetitions) && (0 == ret) ;
                r++) {

        p = data->partidxbuf[r];
        
        ((float*)(bufs[p].arr))[bufs[p].idx] = (float)valdbl;
        bufs[p].idx++;
        if (bufs[p].idx == bufs[p].len) {

            ret = rba_buf_simple_flush (&(bufs[p]));
            if (0 != ret) {
                RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                ret = -1;
            }
        }
    }


    return ret;
}

int
rba_type_double_parse ( rba_data_t  *data,
                        uint32_t    col,
                        const char  *string)
{
    int ret;
    double valdbl;

    ret = strtodouble (string, &valdbl);
    if (-1 == ret) {
        RBA_ERR("failed to convert %s to double\n", string);
    } else {
        uint32_t    r, p;
        rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

        for (r=0, ret=0; \
                (r < data->repetitions) && (0 == ret) ;
                    r++) {

            p = data->partidxbuf[r];
            
            ((double*)(bufs[p].arr))[bufs[p].idx] = valdbl;
            bufs[p].idx++;
            if (bufs[p].idx == bufs[p].len) {

                ret = rba_buf_simple_flush (&(bufs[p]));
                if (0 != ret) {
                    RBA_ERR("rba_buf_simple_flush_if_full failed for col: %u, part: %u\n", (unsigned)col, (unsigned)p);
                    ret = -1;
                }
            }
        }
    }

    return ret;
}

/*
magic numbers:

<IGNORE>                                0x0000000000000000
RBUINT8     52 42 55 49 4e 54 38 00     0x0038544E49554252
RBINT8      52 42 49 4e 54 38 20 20     0x000038544E554252
RBUINT16    52 42 55 49 4e 54 31 36     0x3631544E49554252
RBINT16     52 42 49 4e 54 31 36 00     0x003631544E554252
RBUINT32    52 42 55 49 4e 54 33 32     0x3233544E49554252
RBINT32     52 42 49 4e 54 33 32 00     0x003233544E554252
RBUINT64    52 42 55 49 4e 54 36 34     0x3436544E49554252
RBINT64     52 42 49 4e 54 36 34 00     0x003436544E554252
RBFLOAT     52 42 46 4c 4f 41 54 00     0x0054414F4C464252
RBDOUBLE    52 42 44 4f 55 42 4c 45     0x454C42554F444252

*/

rba_type_t rba_type_ignore ={   .specname   = "ignore",
                                .magic      = 0x0000000000000000,
                                .size       = 0,
                                .initbuf    = rba_type_ignore_initbuf,
                                .freebuf    = rba_type_ignore_freebuf,
                                .parse      = rba_type_ignore_parse};

rba_type_t rba_type_u8 =    {   .specname   = "uint8",
                                .magic      = 0x0038544E49554252,
                                .size       = sizeof(uint8_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_u8_parse};

rba_type_t rba_type_i8 =    {   .specname   = "int8",
                                .magic      = 0x000038544E554252,
                                .size       = sizeof(int8_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_i8_parse};

rba_type_t rba_type_u16 =   {   .specname   = "uint16",
                                .magic      = 0x3631544E49554252,
                                .size       = sizeof(uint16_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_u16_parse};

rba_type_t rba_type_i16 =   {   .specname   = "int16",
                                .magic      = 0x003631544E554252,
                                .size       = sizeof(int16_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_i16_parse};

rba_type_t rba_type_u32 =   {   .specname   = "uint32",
                                .magic      = 0x3233544E49554252,
                                .size       = sizeof(uint32_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_u32_parse};

rba_type_t rba_type_i32 =   {   .specname   = "int32",
                                .magic      = 0x003233544E554252,
                                .size       = sizeof(int32_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_i32_parse};

rba_type_t rba_type_u64 =   {   .specname   = "uint64",
                                .magic      = 0x3436544E49554252,
                                .size       = sizeof(uint64_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_u64_parse};

rba_type_t rba_type_i64 =   {   .specname   = "int64",
                                .magic      = 0x003436544E554252,
                                .size       = sizeof(int64_t),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_i64_parse};

rba_type_t rba_type_float = {   .specname   = "float",
                                .magic      = 0x0054414F4C464252,
                                .size       = sizeof(float),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_float_parse};

rba_type_t rba_type_double ={   .specname   = "double",
                                .magic      = 0x454C42554F444252,
                                .size       = sizeof(double),
                                .initbuf    = rba_buf_alloc,
                                .freebuf    = rba_buf_simple_free,
                                .parse      = rba_type_double_parse};

/*
rba_type_t rba_type_string =    {   .specname   = "uint8",
                                    .size       = sizeof(uint8_t),
                                    .ctx        = NULL,
                                    .initbuf    = rba_type_uint_init,
                                    .freebuf    = rba_type_uint_free,
                                    .parse      = rba_type_uint_parse,
                                    .wrspec     = NULL,
                                    .rdspec     = NULL};

rba_type_t rba_type_enum =  {   .specname   = "uint8",
                                .size       = sizeof(uint8_t),
                                .ctx        = NULL,
                                .initbuf    = rba_type_uint_init,
                                .freebuf    = rba_type_uint_free,
                                .parse      = rba_type_uint_parse,
                                .wrspec     = NULL,
                                .rdspec     = NULL};
*/
