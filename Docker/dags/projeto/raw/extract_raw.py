from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
        )

df = (spark
      .read
      .format("csv")
      .option("header", "true")
      .load("/usr/local/airflow/data")
)

df.printSchema()

df = (df
      .withColumn('Refdate', fn.to_date(fn.col('Refdate'), 'dd/MM/yyyy'))
      )

df.printSchema()

print('Lendo todas as datas com dados armazenados no Minio\n')
df.select('Refdate').distinct().show()

df.show(10, False)

(df
 .write
 .format('parquet')
 .mode('overwrite')
 .partitionBy('Refdate')
 .save('s3a://raw/data')
)

