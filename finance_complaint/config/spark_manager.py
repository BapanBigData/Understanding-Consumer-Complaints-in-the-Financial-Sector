import os
from pyspark.sql import SparkSession
from finance_complaint.constant.env_constant import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from finance_complaint.logger import logger


access_key_id = AWS_ACCESS_KEY_ID
secret_access_key = AWS_SECRET_ACCESS_KEY

#
# spark = SparkSession.builder.master('local[*]').appName('finance_complaint') .getOrCreate()
# hadoop_conf = spark._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# hadoop_conf.set("fs.s3n.awsAccessKeyId", access_key_id)
# hadoop_conf.set("fs.s3n.awsSecretAccessKey", secret_access_key)
#
# spark_session=spark
#


# spark_session = SparkSession.builder.master('local[*]').appName('finance_complaint') \
#     .config("spark.executor.instances", "1") \
#     .config("spark.executor.memory", "6g") \
#     .config("spark.driver.memory", "6g") \
#     .config("spark.executor.memoryOverhead", "8g") \
#     .config('spark.jars.packages',"com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3")\
#     .getOrCreate()



spark_session = SparkSession.builder \
        .appName("financialComplaintApp") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.master", "local[*]") \
        .getOrCreate()


logger.info("**Spark Session created successfully!**")

# spark_session._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", access_key_id)
# spark_session._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", secret_access_key)
# spark_session._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
# spark_session._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# spark_session._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
# spark_session._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
# spark_session._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "ap-south-1.amazonaws.com")
# spark_session._jsc.hadoopConfiguration().set(" fs.s3.buffer.dir","tmp")

