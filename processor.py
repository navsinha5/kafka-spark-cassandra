from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window, from_json, udf, round
from pyspark.sql.types import *
import os


KAFKA_TOPIC = 'payment_msg'
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:29092')
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'localhost')
PROVINCES = ('BH', 'MP', 'UP', 'DL', 'UK', 'JH', 'AP')


# create spark session
spark = SparkSession.Builder() \
                    .appName('processor') \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()


spark.sparkContext.setLogLevel('ERROR')


# define kafka source data schema
source_schema = StructType([
    StructField('createTime', TimestampType()),
    StructField('orderId', IntegerType()),
    StructField('payAmount', DoubleType()),
    StructField('payPlatform', IntegerType()),
    StructField('provinceId', IntegerType())
])


# udf for converting to province name
@udf(returnType=StringType())
def to_province(provinceId):
    if provinceId < 0 or provinceId >= len(PROVINCES):
        return 'NA'
    else:
        return PROVINCES[provinceId]


# configure kafka as data stream source
df_payment_source = spark.readStream \
                        .format('kafka') \
                        .option('kafka.bootstrap.servers', KAFKA_BROKER) \
                        .option('startingOffsets', 'latest') \
                        .option('subscribe', KAFKA_TOPIC) \
                        .load()


# deserialize from default kafka source schema to defined schema
df_payment_source = df_payment_source \
                            .select(
                                from_json(col('value').cast('string'), source_schema) \
                                .alias('value')) \
                            .select('value.*')


# process data
df_payment_processed = df_payment_source \
                            .withWatermark('createTime', '10 second') \
                            .groupBy(
                                window(col('createTime'), '1 minute'),
                                'provinceId') \
                            .agg(round(sum('payAmount'), 2).alias('total')) \
                            .withColumn('province', to_province(col('provinceId')))


# write to console
query_console = df_payment_processed.writeStream \
                    .format('console') \
                    .outputMode('update') \
                    .start()


# write to Cassandra
query_cassandra = df_payment_processed.select('provinceId', 'province', 'total') \
                .writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .outputMode('complete') \
                .option('spark.cassandra.connection.host', CASSANDRA_HOST) \
                .options(keyspace="province", table='pay_amount') \
                .option('confirm.truncate', True) \
                .option('checkpointLocation', '/tmp/checkpoint') \
                .start()


# write to delta table
query_delta = df_payment_processed.writeStream \
                    .format('delta') \
                    .outputMode('append') \
                    .option('checkpointLocation', '/tmp/delta/province/pay_amount/_checkpoint') \
                    .start('/tmp/delta/province/pay_amount')


query_console.awaitTermination()
query_cassandra.awaitTermination()
query_delta.awaitTermination()

