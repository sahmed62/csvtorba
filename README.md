# `csvtorba`

Simple C utility to convert CIC Flow metere data set from CSV files to a set of
raw binary array (RBA) files. The intent is to represent data in a format that
is much fater to load into python for use in data-mining applications.

## Usage:

```
cicfmcsvtorba <partitions> <repetition> <output path> <CSV1> [<CSV2> ...]
```

Example:
```
$ cicfmcsvtorba 16 1 ../../partitioned_rba_16p/ ./*.csv
```

