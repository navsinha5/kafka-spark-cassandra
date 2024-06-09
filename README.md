# Execution Steps
## Step 1
Boot up the cluster  
```bash
$ mkdir -p /tmp/data/kafka /tmp/data/cassandra /tmp/data/delta
$ chown 1001 /tmp/data/delta
$ sudo docker compose up
```

## Step 2
Create Cassandra table
```bash
$ $CASSANDRA_HOME/bin/cqlsh
cqlsh> create keyspace province WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
cqlsh> create table province.pay_amount ("provinceId" int PRIMARY KEY, province text, total double);
```

## Step 3
Start source data generation
```bash
$ pip install -r requirements.txt
$ python generate_source_data.py
```

## Step 4
Submit the Spark data processing job

### Run localy  
```bash
$ $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,\
com.github.jnr:jnr-posix:3.1.19,\
io.delta:delta-spark_2.12:3.2.0 \
--master local \
processor.py
```

### On the local cluster
```bash
$ sudo docker exec -it spark-driver /bin/bash
$ ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,\
io.delta:delta-spark_2.12:3.2.0 \
--master spark://spark-master:7077 \
/app/processor.py
```

# Spark SQL shell for Delta Table
```bash
$ $SPARK_HOME/bin/spark-sql --master local --packages io.delta:delta-spark_2.12:3.2.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
spark-sql> SELECT * FROM delta.`/tmp/data/delta/province/pay_amount`;
```