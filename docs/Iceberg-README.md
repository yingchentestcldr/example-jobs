# HDFS directory structure for an Iceberg table

```bash
[root@ccycloud-1.xhu-718.root.comops.site ~]# hdfs dfs -ls /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1
Found 2 items
drwxr-xr-x   - cdp_xhu hive          0 2024-08-15 19:44 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/data
drwxr-xr-x   - cdp_xhu hive          0 2024-08-15 19:44 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata
```

a sample listing of metadata directory

```bash
[root@ccycloud-1.xhu-718.root.comops.site ~]# hdfs dfs -ls /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata
Found 12 items
-rw-r--r--   2 cdp_xhu hive       2662 2024-08-15 19:41 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/00000-df28e756-51a6-46d3-adc9-c230d376562f.metadata.json
-rw-r--r--   2 cdp_xhu hive       3713 2024-08-15 19:44 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/00001-73bca9fd-8300-47cd-bca0-4256a020928f.metadata.json
-rw-r--r--   2 cdp_xhu hive       4765 2024-08-15 20:07 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/00002-6593d389-ecb3-480a-b80e-ba0229778545.metadata.json
-rw-r--r--   2 cdp_xhu hive       5817 2024-08-15 20:24 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/00003-1d9e818d-e6a8-42d7-acc4-fd7114751f1c.metadata.json
-rw-r--r--   2 cdp_xhu hive       6151 2024-08-15 20:24 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/6eb9b9d9-ac2c-4e6c-8bfc-a81e1fc1021d-m0.avro
-rw-r--r--   2 cdp_xhu hive       6153 2024-08-15 19:41 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/831a3bca-fa48-4110-95e0-4cf40f6895da-m0.avro
-rw-r--r--   2 cdp_xhu hive       6155 2024-08-15 19:44 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/8869da32-93dc-4873-9e4e-bc5f9331cce6-m0.avro
-rw-r--r--   2 cdp_xhu hive       6153 2024-08-15 20:07 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/8e272e80-0578-4399-9760-e0b818022bef-m0.avro
-rw-r--r--   2 cdp_xhu hive       3829 2024-08-15 19:44 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/snap-4428199092346228068-1-8869da32-93dc-4873-9e4e-bc5f9331cce6.avro
-rw-r--r--   2 cdp_xhu hive       3827 2024-08-15 20:24 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/snap-4961869317129186931-1-6eb9b9d9-ac2c-4e6c-8bfc-a81e1fc1021d.avro
-rw-r--r--   2 cdp_xhu hive       3831 2024-08-15 19:41 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/snap-7718760204238514918-1-831a3bca-fa48-4110-95e0-4cf40f6895da.avro
-rw-r--r--   2 cdp_xhu hive       3829 2024-08-15 20:07 /warehouse/tablespace/external/hive/retail_cdp_xhu.db/tokenized_iceberg1/metadata/snap-8125264318880362410-1-8e272e80-0578-4399-9760-e0b818022bef.avro
```
