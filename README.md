# pure-tpcds-test
This test does not dependent on any thirdparty libraries.
It only dependent on spark and jdk.

## Compile
If you dependent on other spark dependency, modify the following part in pom.xml.
And run `mvn clean package`, copy generated files in target directory.
```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.1.1</version>
</dependency>
```

## Run TPCDS
Only one parameter: the database name
```bash
spark-submit  \
 --master yarn \
 --deploy-mode cluster \
 --num-executors 72 \
 --executor-cores 2 \
 --driver-memory 6G \
 --conf spark.app.name=spark-3.2.0-tpcds \
 --conf spark.yarn.am.memory=2g \
 --conf spark.yarn.max.executor.failures=1 \
 --conf spark.executor.memoryOverhead=1g \
 --conf spark.memory.offHeap.enabled=true \
 --conf spark.memory.offHeap.size=5g \
 --executor-memory 2G  \
 --conf spark.yarn.maxAppAttempts=1  \
 --conf spark.sql.catalogImplementation=hive  \
 --conf spark.sql.warehouse.dir=/apps/spark/warehouse  \
 --conf spark.shuffle.useOldFetchProtocol=true  \
 --conf spark.yarn.maxAppAttempts=1  \
 --class org.houzhizhen.tpcds.PureJavaTpcdsTest \
  ./pure-tpcds-test-1.0-SNAPSHOT.jar tpcds_sf1_withdecimal_withdate_withnulls
```

## Run Specified SQl File
```bash
/opt/bmr/spark-3.2.0/bin/spark-submit  \
 --master local \
 --class org.houzhizhen.tpcds.ExecuteSqlFile \
  ./pure-tpcds-test-1.0-SNAPSHOT.jar \
  -f hdfs://bmr-cluster/houzhizhen/test/test.sql
```