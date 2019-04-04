Converts hive table scheme to pyspark DataFrame scheme

__From this__:
```sql
CREATE TABLE `sandbox.phones`(
      `phone` bigint comment "Additional hive column Comment",
      `phone_hash` string)
```
__To this__:
```python
StructType([
        StructField("phone", LongType()),
        StructField("phone_hash", StringType()),
)]
```

----

### Example
Hive "show create table sandbox.phones;" command returns following text:
```python
out = """
    createtab_stmt
    CREATE TABLE `sandbox.phones`(
      `phone` bigint comment "Additional hive column Comment",
      `phone_hash` string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION
      'hdfs://hdp-master-03.local:8000/apps/hive/warehouse/sandbox.db/phones'
    TBLPROPERTIES (
      'COLUMN_STATS_ACCURATE'='{\"COLUMN_STATS\":{\"phone\":\"true\",\"phone_hash\":\"true\"}}',
      'last_modified_by'='username',
      'last_modified_time'='1537870512',
      'numFiles'='27',
      'numRows'='0',
      'rawDataSize'='0',
      'totalSize'='15412994750',
      'transient_lastDdlTime'='1537874214')
    """
```

### Run
Run script with the following command:
`-t` parameter stands for hive table name
```shell
python get_hive_table_scheme_to_pyspark.py -t sandbox.phones
```

### Returns
```python
StructType([
        StructField("phone", LongType()),
        StructField("phone_hash", StringType()),
)]
```
