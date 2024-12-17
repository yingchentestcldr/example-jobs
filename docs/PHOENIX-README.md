# Prerequisite

1. CDP Base cluster setup:
   a. set JAVA_HOME for the user: add the following lines to .bash_profile

   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-openjdk-11
   export PATH=$JAVA_HOME/bin:$PATH
   ```

   b. find phoenix root folder: /opt/cloudera/parcels/CDH/lib/phoenix/bin

2. create a Phoenix table:
   a. `sqlline ccycloud-1.xhu-718.root.comops.site:2181`
   b. `CREATE TABLE TABLE1 (ID BIGINT NOT NULL PRIMARY KEY, COL1 VARCHAR);`
   c. `UPSERT INTO TABLE1 (ID, COL1) VALUES (1, 'test_row_1');`
   d. `UPSERT INTO TABLE1 (ID, COL1) VALUES (2, 'test_row_2');`
3. create a CDE job phoenix-read to read from the Phoenix table
