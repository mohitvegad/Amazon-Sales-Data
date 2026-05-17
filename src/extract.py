from pyspark.sql import SparkSession
from utils.logger import logger

def extract_data(path):
    
    # Spark Session
    spark = SparkSession.builder \
        .appName("Amazon Sales") \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIASGDWR5CRX4CQHN5L")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    

    df = spark.read.csv(path, header=True, inferSchema=True)
    
     # Logging
    logger.info("Extraction completed successfully")

    return df, spark