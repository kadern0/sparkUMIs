from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('test').getOrCreate()

# Credentials for Google Cloud Storage
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","/opt/bitnami/spark/credentials.json")

BUCKET_NAME = "pyspark-sort"
FILENAME = "input_file.sam"
OUTPUTFILE = "result"
path = f"gs://{BUCKET_NAME}/{FILENAME}"
output_path = f"gs://{BUCKET_NAME}/{OUTPUTFILE}"

sc = spark.sparkContext


customSchema = StructType([
    StructField("QNAME", StringType(), True),
    StructField("FLAG", StringType(), True),
    StructField("RNAME", StringType(), True),
    StructField("POS", StringType(), True),
    StructField("MAPQ", StringType(), True),
    StructField("CIGAR", StringType(), True),
    StructField("RNEXT", StringType(), True),
    StructField("PNEXT", StringType(), True),
    StructField("TLEN", StringType(), True),
    StructField("SEQ", StringType(), True),
    StructField("QUAL", StringType(), True),
    StructField("NH", StringType(), True),
    StructField("HI", StringType(), True),
    StructField("AS", StringType(), True),
    StructField("nM", StringType(), True),
    StructField("XS", StringType(), True),
    StructField("XN", StringType(), True),
    StructField("XT", StringType(), True)])



parDF=spark.read.parquet(path)

parDF.sortWithinPartitions(
    col('RNAME'), col('POS').asc()).write.parquet(output_path)
