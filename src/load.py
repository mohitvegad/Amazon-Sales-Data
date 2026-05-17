
def save_to_s3(df, path):
    # df.write.mode("overwrite").parquet(path)
    df.write \
      .mode("overwrite") \
      .option("header", True) \
      .csv(path)