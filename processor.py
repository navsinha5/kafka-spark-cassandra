from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window, from_json, udf
from pyspark.sql.types import *
import os


KAFKA_TOPIC = 'payment_msg'
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'kafka:9092')
PROVINCES = ('BH', 'MP', 'UP', 'DL', 'UK', 'JH', 'AP')


# create spark session
spark = SparkSession.Builder() \
                    .appName('processor') \
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
                        .option('kafka.bootstrap.servers', KAFKA_SERVER) \
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
                            .groupBy(
                                window(df_payment_source['createTime'], '1 minute'),
                                'provinceId') \
                            .agg(sum('payAmount').alias('total')) \
                            .withColumn('province', to_province(df_payment_source['provinceId']))


query_console = df_payment_processed.writeStream \
                    .format('console') \
                    .outputMode('complete') \
                    .start()

query_cassandra = df_payment_processed.select('provinceId', 'province', 'total') \
                .writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .outputMode('complete') \
                .options(keyspace="test", table='sink') \
                .option('confirm.truncate', True) \
                .option('checkpointLocation', '/tmp/checkpoint') \
                .start()


query_console.awaitTermination()
query_cassandra.awaitTermination()

