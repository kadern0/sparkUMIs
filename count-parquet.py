"""pySpark_Parquet_UMI_count"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import substring
from pyspark.sql.functions import col, udf
from collections import Counter
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('test').getOrCreate()

# Credentials to write on Google Cloud Storage
spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.json.keyfile",
    "/opt/bitnami/spark/credentials.json")

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

def hamming_distance(str1, str2):
    """Returns hamming distance between two strings"""
    return len(list(filter(lambda x: ord(x[0]) ^ ord(x[1]), zip(str1, str2))))


def list_to_dict_sorted(lst):
    return dict(sorted(dict(
        Counter(lst)).items(), key=lambda x: x[1], reverse=True))

def uniqueness(input):
    input_dictionary = list_to_dict_sorted(input)
    output_list = []
    output_list.append(next(iter(input_dictionary)))
    input_dictionary.pop(next(iter(input_dictionary), None))
    for item in input_dictionary.keys():
        is_unique = True
        for unique_item in output_list:
            if hamming_distance(item, unique_item) < 2:
                is_unique = False
        if is_unique:
            output_list.append(item)
    return len(output_list)


udf_uniqu = udf(uniqueness, IntegerType())
parseQname = udf(lambda x: x.split('_')[1])
parseCell = udf(lambda x: x.split('_')[2])

parDF = spark.read.parquet(path)

newDF = parDF.filter(col('XT').isNotNull()).withColumn(
    'UMI', parseCell(col('QNAME'))).withColumn(
        'cell', parseQname(col('QNAME'))).groupBy(
            'RNAME', 'cell', 'XT').agg(
                F.collect_list('UMI').alias('UMIs'))

readsDF = newDF.withColumn(
    "reads", udf_uniqu(col("UMIs"))).withColumn(
        'gene', substring(col('XT'), 6, 26)).select(
            'gene', 'cell', 'reads')

readsDF.write.parquet(output_path)
