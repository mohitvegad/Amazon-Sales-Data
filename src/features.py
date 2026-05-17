from pyspark.sql.functions import avg, count, max, when, col
from pyspark.sql.window import Window

def build_features(product_df):

#------------------------------------------------------
# Rating features    
#------------------------------------------------------

    # product_df.orderBy(col("actual_price").desc()).show(10, False)

#------------------------------------------------------
# Rating features    
#------------------------------------------------------

    product_df = product_df.withColumn(
    "rating_bucket",
        when(col("rating") >= 4.5, "excellent")
        .when(col("rating") >= 4.0, "good")
        .when(col("rating") >= 3.0, "average")
        .otherwise("poor")
    )
    
    product_df = product_df.orderBy(col("actual_price").desc())
    
    return product_df