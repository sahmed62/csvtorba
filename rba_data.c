#include <ctype.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <rba.h>


char*
rba_strtrim (char *string)
{
    char *cp = string;

    /*  find the end of the string */
    for (cp = string; cp[0] != '\0'; cp++);
    /*  if not zero-length string, trim trailing spaces*/
    for (cp--; (cp >= string) && isspace(cp[0]); cp--) {
        cp[0] = '\0';
    }
    /*  trim leading spaces */
    for (cp = string; \
            (cp[0] != '\0') && isspace(cp[0]); \
                cp++);
    return cp;
}

static int
rba_checkhdr (  rba_spec_entry_t    *spec,
                uint32_t            cols,
                FILE*               filep)
{
    int ret;

    char *line, *iterator, *token;
    size_t bufsz;
    size_t token_idx;
    ssize_t len;

    line = NULL;
    bufsz = 0;
    len = getline(&line, &bufsz, filep);
    if (len < 0) {
        RBA_ERR("Failed to read line header\n");
        RBA_ERRNO();
        ret = -1;
    } else {
        ret = 0;
        token_idx = 0;
        for_each_csvtoken(line, iterator, token) {

            token = rba_strtrim (token);
            if (token_idx >= cols) {
                RBA_ERR("Header contains more columns than expected: \"%s\"\n", token);
                ret = -1;
                break;
            } else {

                if (0 != strcmp (spec[token_idx].name, token)) {
                    RBA_ERR("Header mismatch for column %u. expected \"%s\", got \"%s\".\n", (unsigned)token_idx, spec[token_idx].name, token);
                    ret = -1;
                    break;
                } else {

                    token_idx++;
                }
            }
        }

        if (token_idx < cols) {
            RBA_ERR("Header contains fewer columns (%u) than expected (%u) \n", (unsigned)token_idx, (unsigned)cols);
            ret = -1;
        }

        free (line);
    }

    return ret;
}

int
rba_checkhdr_countrecords ( rba_spec_entry_t    *spec,
                            uint32_t            cols,
                            const char          *filename,
                            uint64_t            *reccount_p)
{
    int ret;

    char *buf, *bufend, *nextnewline;
    size_t readin;
    uint64_t reccount;

    FILE* filep = fopen (filename, "r");
    if (NULL == filep) {
        RBA_ERR("Failed to open file %s\n", filename);
        ret = -1;
    } else {
        ret = rba_checkhdr (spec,
                            cols,
                            filep);
        if (0 != ret) {
            RBA_ERR("Header check for CSV file %s failed\n", filename);
            ret = -1;
        } else {
            buf = malloc(16*1024*1024);
            if (NULL == buf) {
                RBA_ERR("Failed to allocate 1MB buffer\n");
                ret = -1;
            } else {

                reccount = 0;
                ret = 0;
                do {
                    readin = fread (buf, 1, (16*1024*1024), filep);
                    if ((readin < (16*1024*1024)) && ferror(filep)) {
                        RBA_ERR("Error reading file\n");
                        ret = -1;
                    } else {
                        if (readin > 0) {
                            nextnewline = buf;
                            bufend = buf + readin;
                            do{
                                nextnewline = memchr (  nextnewline,
                                                        '\n',
                                                        (bufend-nextnewline));
                                if (NULL != nextnewline) {
                                    nextnewline++;
                                    reccount++;
                                }
                            } while (NULL != nextnewline);
                        }
                    }
                } while((ret == 0) && (readin == (16*1024*1024)));

                if (0 == ret) {
                    *reccount_p = reccount;
                }

                free(buf);
            }
        }

        fclose(filep);
    }

    return ret;
}

/*  linear congruential generator (X = X*C + A mod M) as fast PRNG */
#define RBA_LCG_X0 (1)
#define RBA_LCG_A (6364136223846793005ULL)
#define RBA_LCG_C (1ULL)
#define RBA_LCG_INIT(X) (X = RBA_LCG_X0)
#define RBA_LCG_NEXT(X) (X = ((X) * RBA_LCG_A + RBA_LCG_C) & 0xFFFFFFFFFFFFFFFF)
#define RBA_LCG_MAX (18446744073709551616.0)
#define LCG_GET_DOUBLE(X) ((double)(X) / RBA_LCG_MAX)
#define LCG_GET_INRANGE(X, RANGEMIN, RANGEMAX) ((uint64_t)(LCG_GET_DOUBLE(X) * (double)(RANGEMAX -RANGEMIN)) + RANGEMIN)

static int
init_partpicker(rba_data_t  *data,
                uint64_t    total_samples)
{
    int ret;
    uint32_t samples_par_partition;
    uint32_t arrlen = data->partitions + data->repetitions;
    uint32_t p;

    data->partsmpl_remaining = (uint32_t*)malloc (arrlen*sizeof(uint32_t));
    if (NULL == data->partsmpl_remaining) {
        RBA_ERR("malloc failed for uint32_t array of length %u\n", (unsigned)arrlen);
        ret = -1;
    } else {

        data->partidxbuf = data->partsmpl_remaining + data->partitions;

        RBA_LCG_INIT(data->rng_state);
        data->totsmpl_remaining = total_samples * data->repetitions;
        samples_par_partition = data->totsmpl_remaining / data->partitions;

        for (p = 0; p < data->partitions; p++) {
            data->partsmpl_remaining[p] = samples_par_partition;
        }

        ret = 0;
    }

    return ret;
}

static void
pick_next_partitions(rba_data_t *data)
{
    uint32_t r, p;
    uint64_t rem_pick_idx;
    for(r=0; r <data->repetitions; r++) {
        /* pick a random number from 0 to totsmpl_remaining */
        RBA_LCG_NEXT(data->rng_state);
        rem_pick_idx = LCG_GET_INRANGE(data->rng_state, 1, data->totsmpl_remaining);
        /* find which partition it falls in */
        for (p=0;
                rem_pick_idx > data->partsmpl_remaining[p];
                    rem_pick_idx -= data->partsmpl_remaining[p], p++);
        /* select the partition */
        data->partidxbuf[r] = p;
        /* decrement the samples remaining for that partition */
        data->partsmpl_remaining[p]--;
        /* decrement the total samples remaining */
        data->totsmpl_remaining--;       
    }
}


static int
rba_data_setup_dir_structure (  const char  *dirpath,
                                uint32_t    partitions,
                                uint32_t    cols,
                                char**      filepath_buf_p,
                                size_t*     filepathlen_p)
{
    int ret;

    uint32_t p, c;

    size_t filepathlen;
    size_t dirpathlen = strlen(dirpath);

    char *filepath_buf;

    FILE* filep;

    printf ("    Creating directory structure under \"%s\"\n", dirpath);

    filepathlen = dirpathlen + strlen("/p00000000/c00000000.bin") + 1;

    filepath_buf = (char*)malloc(filepathlen * sizeof(char));
    if (NULL == filepath_buf) {
        RBA_ERR("Failed to allocate %u bytes for file path\n", (unsigned)filepathlen);
        ret = -1;
    } else {
        strcpy(filepath_buf, dirpath);

        /* create the root directory */
        ret = mkdir(filepath_buf, 0777);
        if (0 != ret) {
            RBA_ERR("Failed to create root RBA directory %s\n", filepath_buf);
            RBA_ERRNO();
            ret = -1;
        } else {

            for (p = 0; (p < partitions) && (0 == ret); p++) {
                ret = snprintf (filepath_buf,
                                filepathlen,
                                "%s/p%08X",
                                dirpath,
                                p);
                if (ret < 0) {
                    RBA_ERR("Failed to create RBA partition directory path %08x\n", p);
                    ret = -1;
                } else {

                    /* create the partition directory */
                    ret = mkdir(filepath_buf, 0777);
                    if (0 != ret) {
                        RBA_ERR("Failed to create RBA partition directory %s\n", filepath_buf);
                        RBA_ERRNO();
                        ret = -1;
                    } else {

                        for (c = 0; (c < cols) && (0 == ret); c++) {
                            ret = snprintf (filepath_buf,
                                            filepathlen,
                                            "%s/p%08X/c%08X.bin", dirpath, p, c);
                            if (ret < 0) {
                                RBA_ERR("Failed to create RBA file path p%08X/c%08X.bin\n", p, c);
                                ret = -1;
                            } else {

                                /* create an empty file */
                                filep = fopen(filepath_buf, "wb");
                                if (NULL == filep) {
                                    RBA_ERR("Failed to create RBA file %s\n", filepath_buf);
                                    RBA_ERRNO();
                                    ret = -1;
                                } else {
                                    ret = 0;
                                    fclose(filep);
                                }
                            }
                        }
                    }
                }
            }
        }

        if (0 != ret) {
            free (filepath_buf);
        } else {
            *filepath_buf_p = filepath_buf;
            *filepathlen_p = filepathlen;
        }
    }

    return ret;
}

int
rba_data_alloc (rba_data_t          *data,
                rba_spec_entry_t    *spec,
                uint32_t            cols,
                const char          *dirpath,
                uint32_t            partitions,
                uint32_t            repetitions,
                uint64_t            samples)
{
    int ret;

    uint32_t c, p;

    char *filepath_buf;
    size_t filepathlen;

    size_t buf_count;

    rba_buf_t *bufs;
    rba_type_t *type;

    ret = rba_data_setup_dir_structure (dirpath,
                                        partitions,
                                        cols,
                                        &filepath_buf,
                                        &filepathlen);
    if (0 != ret) {
        RBA_ERR("Failed to setup directory structure under %s\n", dirpath);
        ret = -1;
    } else {
        ret = 0;

        data->spec = spec;
        data->cols = cols;
        data->partitions = partitions;
        data->repetitions = repetitions;
        ret = init_partpicker(data, samples);
        if (0 != ret) {
            RBA_ERR("init_partpicker failed\n");
            ret = -1;
        } else {

            buf_count = cols * partitions;
            data->bufs = (rba_buf_t*)malloc(buf_count * sizeof(rba_buf_t));
            if (NULL == data->bufs) {
                RBA_ERR("Failed to allocate memory for the rba_buf_t array.\n");
                ret = -1;
            } else {

                memset (data->bufs, 0, buf_count * sizeof(rba_buf_t));

                for (c=0; (c < data->cols) && (0 == ret); c++) {

                    bufs = rba_data_getcolbufs(data, c);
                    type = data->spec[c].type;

                    for (p = 0; (p < data->partitions) && (0 == ret); p++) {

                        ret = snprintf (filepath_buf,
                                        filepathlen,
                                        "%s/p%08X/c%08X.bin", dirpath, p, c);
                        if (ret < 0) {
                            RBA_ERR("Failed to create RBA file path p%08X/c%08X.bin\n", p, c);
                            ret = -1;
                        } else {

                            ret = type->initbuf (   type,
                                                    filepath_buf,
                                                    &(bufs[p]));
                            if (0 != ret) {
                                RBA_ERR("Failed to initialize rba_buf_t for column %u, partition %u\n", (unsigned)p, (unsigned)c);
                                ret = -1;
                            } else {
                                ret = 0;
                            }
                        }
                    }
                }

                if (0 != ret) {
                    for (c=0; (c < data->cols); c++) {
                        bufs = rba_data_getcolbufs(data, c);
                        type = data->spec[c].type;

                        for (p = 0; (p < data->partitions); p++) {
                            if (0 != bufs[p].len) {
                                fprintf(stderr, "freeing %u %u", c, p);
                                type->freebuf(type, &(bufs[p]));
                            }
                        }
                    }
                    free (data->bufs);
                }
            }

            if (0 != ret) {
                free (data->partsmpl_remaining);
                memset (data, 0, sizeof(rba_data_t));
            }
        }

        free (filepath_buf);
    }

    return ret;
}

extern int
rba_data_parse_line (   rba_data_t  *data,
                        char        *nextline)
{
    int ret = 0;

    char *iterator, *token;

    uint32_t c;

    rba_type_t *type;

    pick_next_partitions(data);

    c = 0;
    for_each_csvtoken(nextline, iterator, token) {
        token = rba_strtrim (token);
        type = data->spec[c].type;
        ret = type->parse ( data,
                            c,
                            token);
        if (0 != ret) {
            RBA_ERR("failed to parse column (%u) \"%s\"\n", (unsigned)c, token);
            ret = -1;
            break;
        } else {
            c++;
            if (c > data->cols) {
                RBA_ERR("line contains more columns columns (%u) than expected (%u) \n", (unsigned)c, (unsigned)data->cols);
                ret = -1;
                break;
            }
        }
    }

    if ((0 == ret) && (c < data->cols)) {
        RBA_ERR("line contains fewer columns (%u) than expected (%u) \n", (unsigned)c, (unsigned)data->cols);
        ret = -1;
    }

    return ret;
}

extern int
rba_data_parse_csvs (   rba_data_t  *data,
                        const char  **csvnames,
                        int         csvcount)
{
    int ret;

    FILE *filep;

    char    *nextline;
    size_t  buf_sz;
    ssize_t  line_sz;

    int csv_idx;

    uint64_t lineno;

    nextline = NULL;
    buf_sz = 0;

    for (csv_idx = 0, ret = 0; (csv_idx < csvcount) && (0 == ret); csv_idx++) {
        
        filep = fopen(csvnames[csv_idx], "r");
        if (NULL == filep) {

            RBA_ERR("Failed to open CSV flie %s\n", csvnames[csv_idx]);
            RBA_ERRNO();
            ret = -1;
        } else {

            lineno = 0;
           /* Read the header */
            line_sz = getline(&nextline, &buf_sz, filep);
            if (line_sz < 0) {

                RBA_ERR("Failed to read header file from CSV file %s\n", csvnames[csv_idx]);
                RBA_ERRNO();
                ret = -1;
            } else {
                printf("    Parsing CSV file %s\n", csvnames[csv_idx]);

                /* Read the remaining lines */
                do {
                    lineno++;
                    line_sz = getline(&nextline, &buf_sz, filep);
                    if (line_sz > 0) {
                        ret = rba_data_parse_line (data, nextline);
                        if (0 != ret) {

                            RBA_ERR("Failed to parse line %llu from CSV file %s\n", (unsigned long long)lineno, csvnames[csv_idx]);
                            ret = -1;
                        }
                    }
                } while((line_sz >= 0) && (0 == ret));
            
                if (0 != errno) {
                    RBA_ERR("Error parsing CSV file %s\n", csvnames[csv_idx]);
                    ret = -1;
                }
            }

            fclose (filep);
        }
    }

    if (buf_sz > 0) {
        free(nextline);
    }

    return ret;
}

int
rba_data_free (rba_data_t *data)
{
    int ret = 0;
    
    uint32_t c, p;

    rba_buf_t *bufs;
    rba_type_t *type;

    for (c=0; c < data->cols; c++) {
        bufs = rba_data_getcolbufs(data, c);
        type = data->spec[c].type;

        for (p = 0; p < data->partitions; p++) {
            type->freebuf(type, &(bufs[p]));
        }
    }
    /*free (data->bufs);*/
    free (data->partsmpl_remaining);
    memset (data, 0, sizeof(rba_data_t));
    return ret;
}

