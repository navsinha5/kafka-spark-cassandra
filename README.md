## Step 1
Boot up the cluster  
`$ sudo docker compose up`

## Step 2
Create Cassandra table
```bash
$ $CASSANDRA_HOME/bin/cqlsh
cqlsh> create keyspace test WITH replication = {‘class’:’SimpleStrategy’, ‘replication_factor’ : 1};
cqlsh> create table test.sink ("provinceId" int PRIMARY KEY, province text, total double);
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
$ export KAFKA_SERVER=localhost:29092
$ $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,\
com.github.jnr:jnr-posix:3.1.19 \
processor.py
```

### On the local cluster setup
```bash
$ sudo docker exec -it spark-driver /bin/bash
$ cd bin
$ ./spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
--master spark://spark-master:7077 \
/app/processor.py
```