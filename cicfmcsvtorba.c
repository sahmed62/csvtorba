
#include <rba.h>

#define CICFM_LABEL_COUNT (13)
const char  *cicfm_labels[CICFM_LABEL_COUNT] = {"BENIGN",
                                                "DrDoS_DNS",
                                                "DrDoS_MSSQL",
                                                "DrDoS_NTP",
                                                "DrDoS_SSDP",
                                                "Syn",
                                                "UDP-lag",
                                                "WebDDoS",
                                                "DrDoS_LDAP",
                                                "DrDoS_NetBIOS",
                                                "DrDoS_SNMP",
                                                "DrDoS_UDP",
                                                "TFTP"  };

int
rba_type_cicfm_parse (  rba_data_t  *data,
                        uint32_t    col,
                        const char  *string)
{
    int ret;
    uint8_t id;

    for (id = 0; \
        (id < CICFM_LABEL_COUNT) && (0 != strcmp(string, cicfm_labels[id]));
            id++);

    if (CICFM_LABEL_COUNT == id) {
        RBA_ERR("Unknown label for CICFM record: %s\n", string);
        ret = -1;
    } else {
        uint32_t    r, p;
        rba_buf_t   *bufs = rba_data_getcolbufs(data, col);

        for (r=0, ret=0; \
                (r < data->repetitions) && (0 == ret) ;
                    r++) {

            p = data->partidxbuf[r];
            
            ((int8_t*)(bufs[p].arr))[bufs[p].idx] = id;
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

rba_type_t rba_type_cicfm_label =    {  .specname   = "cicfm_label",
                                        .magic      = 0x4142524d46434943,
                                        .size       = sizeof(uint8_t),
                                        .initbuf    = rba_buf_alloc,
                                        .freebuf    = rba_buf_simple_free,
                                        .parse      = rba_type_cicfm_parse};

uint32_t         cicfm_cols = 88;
rba_spec_entry_t cicfm_rbaspec[] =  {   {"Unnamed: 0",                  &rba_type_ignore },
                                        {"Flow ID",                     &rba_type_ignore },
                                        {"Source IP",                   &rba_type_ignore },
                                        {"Source Port",                 &rba_type_float },
                                        {"Destination IP",              &rba_type_ignore },
                                        {"Destination Port",            &rba_type_float },
                                        {"Protocol",                    &rba_type_float },
                                        {"Timestamp",                   &rba_type_ignore },
                                        {"Flow Duration",               &rba_type_float },
                                        {"Total Fwd Packets",           &rba_type_float },
                                        {"Total Backward Packets",      &rba_type_float },
                                        {"Total Length of Fwd Packets", &rba_type_float },
                                        {"Total Length of Bwd Packets", &rba_type_float },
                                        {"Fwd Packet Length Max",       &rba_type_float },
                                        {"Fwd Packet Length Min",       &rba_type_float },
                                        {"Fwd Packet Length Mean",      &rba_type_float },
                                        {"Fwd Packet Length Std",       &rba_type_float },
                                        {"Bwd Packet Length Max",       &rba_type_float },
                                        {"Bwd Packet Length Min",       &rba_type_float },
                                        {"Bwd Packet Length Mean",      &rba_type_float },
                                        {"Bwd Packet Length Std",       &rba_type_float },
                                        {"Flow Bytes/s",                &rba_type_float },
                                        {"Flow Packets/s",              &rba_type_float },
                                        {"Flow IAT Mean",               &rba_type_float },
                                        {"Flow IAT Std",                &rba_type_float },
                                        {"Flow IAT Max",                &rba_type_float },
                                        {"Flow IAT Min",                &rba_type_float },
                                        {"Fwd IAT Total",               &rba_type_float },
                                        {"Fwd IAT Mean",                &rba_type_float },
                                        {"Fwd IAT Std",                 &rba_type_float },
                                        {"Fwd IAT Max",                 &rba_type_float },
                                        {"Fwd IAT Min",                 &rba_type_float },
                                        {"Bwd IAT Total",               &rba_type_float },
                                        {"Bwd IAT Mean",                &rba_type_float },
                                        {"Bwd IAT Std",                 &rba_type_float },
                                        {"Bwd IAT Max",                 &rba_type_float },
                                        {"Bwd IAT Min",                 &rba_type_float },
                                        {"Fwd PSH Flags",               &rba_type_float },
                                        {"Bwd PSH Flags",               &rba_type_float },
                                        {"Fwd URG Flags",               &rba_type_float },
                                        {"Bwd URG Flags",               &rba_type_float },
                                        {"Fwd Header Length",           &rba_type_float },
                                        {"Bwd Header Length",           &rba_type_float },
                                        {"Fwd Packets/s",               &rba_type_float },
                                        {"Bwd Packets/s",               &rba_type_float },
                                        {"Min Packet Length",           &rba_type_float },
                                        {"Max Packet Length",           &rba_type_float },
                                        {"Packet Length Mean",          &rba_type_float },
                                        {"Packet Length Std",           &rba_type_float },
                                        {"Packet Length Variance",      &rba_type_float },
                                        {"FIN Flag Count",              &rba_type_float },
                                        {"SYN Flag Count",              &rba_type_float },
                                        {"RST Flag Count",              &rba_type_float },
                                        {"PSH Flag Count",              &rba_type_float },
                                        {"ACK Flag Count",              &rba_type_float },
                                        {"URG Flag Count",              &rba_type_float },
                                        {"CWE Flag Count",              &rba_type_float },
                                        {"ECE Flag Count",              &rba_type_float },
                                        {"Down/Up Ratio",               &rba_type_float },
                                        {"Average Packet Size",         &rba_type_float },
                                        {"Avg Fwd Segment Size",        &rba_type_float },
                                        {"Avg Bwd Segment Size",        &rba_type_float },
                                        {"Fwd Header Length.1",         &rba_type_float },
                                        {"Fwd Avg Bytes/Bulk",          &rba_type_float },
                                        {"Fwd Avg Packets/Bulk",        &rba_type_float },
                                        {"Fwd Avg Bulk Rate",           &rba_type_float },
                                        {"Bwd Avg Bytes/Bulk",          &rba_type_float },
                                        {"Bwd Avg Packets/Bulk",        &rba_type_float },
                                        {"Bwd Avg Bulk Rate",           &rba_type_float },
                                        {"Subflow Fwd Packets",         &rba_type_float },
                                        {"Subflow Fwd Bytes",           &rba_type_float },
                                        {"Subflow Bwd Packets",         &rba_type_float },
                                        {"Subflow Bwd Bytes",           &rba_type_float },
                                        {"Init_Win_bytes_forward",      &rba_type_float },
                                        {"Init_Win_bytes_backward",     &rba_type_float },
                                        {"act_data_pkt_fwd",            &rba_type_float },
                                        {"min_seg_size_forward",        &rba_type_float },
                                        {"Active Mean",                 &rba_type_float },
                                        {"Active Std",                  &rba_type_float },
                                        {"Active Max",                  &rba_type_float },
                                        {"Active Min",                  &rba_type_float },
                                        {"Idle Mean",                   &rba_type_float },
                                        {"Idle Std",                    &rba_type_float },
                                        {"Idle Max",                    &rba_type_float },
                                        {"Idle Min",                    &rba_type_float },
                                        {"SimillarHTTP",                &rba_type_ignore },
                                        {"Inbound",                     &rba_type_float },
                                        {"Label",                       &rba_type_cicfm_label },
                                    };

const char*
usagestring = "%s <partitions> <repetition> <dirpath> <CSV1> [<CSV2> ...]\n";

int main(int argc, const char **argv)
{
    int ret, exit_ret;

    uint64_t partitions, repetitions;

    const char *dirpath;
    const char **csvlist;
    int csvcount;

    int csv_idx;
    uint64_t total_reccount;
    uint64_t file_reccount;

    rba_data_t data;

    if (argc < 5) {
        fprintf (stderr, usagestring, argv[0]);
        ret = -1;
    } else {

        ret = strtouint64 (argv[1], &partitions);
        if (0 != ret) {
            fprintf (stderr, "ERROR: failed to parse first argument\n");
            fprintf (stderr, usagestring, argv[0]);
        } else {

            ret = strtouint64 (argv[2], &repetitions);
            if (0 != ret) {
                fprintf (stderr, "ERROR: failed to parse second argument\n");
                fprintf (stderr, usagestring, argv[0]);
            } else {
                dirpath = argv[3];
                csvlist = argv + 4;
                csvcount= argc - 4;

                for (csv_idx = 0, total_reccount=0; 
                        ((csv_idx < csvcount) && (0 == ret));
                            csv_idx++) {
                    ret = rba_checkhdr_countrecords (   cicfm_rbaspec,
                                                        cicfm_cols,
                                                        csvlist[csv_idx],
                                                        &file_reccount);
                    if (0 == ret) {
                        printf("    %s is valid and contains %lu records\n",
                                csvlist[csv_idx],
                                file_reccount);
                        total_reccount += file_reccount;
                    }
                    
                }

                if (0 == ret) {
                    printf("    Total number of records:    %lu\n",
                            total_reccount);
                    ret = rba_data_alloc (  &data,
                                            cicfm_rbaspec,
                                            cicfm_cols,
                                            dirpath,
                                            partitions,
                                            repetitions,
                                            total_reccount);
                    if (0 == ret) {
                        ret = rba_data_parse_csvs ( &data,
                                                    csvlist,
                                                    csvcount);
                        if (0 != ret) {
                            fprintf (stderr, "ERROR: failed to parse CSVs!\n");
                            ret = -1;
                        }

                        exit_ret = rba_data_free (&data);
                        if (0 != exit_ret) {
                            fprintf (stderr, "ERROR: clean up failed!\n");
                            ret = -1;
                        }
                    }
                }
            }
        }
    }

    return ret;
}

