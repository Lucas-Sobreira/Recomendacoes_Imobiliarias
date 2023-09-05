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

df_context = (spark
              .read
              .format("parquet")
              .load("s3a://context/data")
              )

df_trust = (df_context
            .orderBy("Refdate", ascending=False)
            .dropDuplicates(["Link_Apto", "Endereco", "Valor_RS"])
            .na.drop()
            .orderBy("Refdate", ascending=False)
            .withColumn("id", fn.monotonically_increasing_id())
           )

df_trust.printSchema()

df_trust.show(10, False)

(df_trust
 .write
 .format('parquet')
 .mode('overwrite')
 .partitionBy('Refdate')
 .save('s3a://trust/particionado')
)

(df_trust
 .write
 .format('parquet')
 .mode('overwrite')
 .save('s3a://trust/unificado')
)