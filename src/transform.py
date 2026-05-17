from pyspark.sql.functions import col, when, regexp_replace, trim, lit

def transform_data(product_df):

    product_df = product_df.select(
        "product_id",
        # "product_name",
        # "category",
        "discounted_price",
        "actual_price",
        "discount_percentage",
        "rating"
    )
    
    # product_df = product_df.select(
    #     "product_id",
    #     "actual_price"
    # )
    
    # ---------------------------------------
    # 1. Remove duplicates
    # ---------------------------------------
    product_df = product_df.dropDuplicates()

    # ---------------------------------------
    # 2. Clean + convert actual_price
    # ---------------------------------------
    product_df = product_df.withColumn(
        "actual_price",
        when(
            trim(regexp_replace(col("actual_price"), "[^0-9.]", "")) == "",
            lit(None)
        ).otherwise(
            regexp_replace(col("actual_price"), "[^0-9.]", "")
        ).cast("double")
    )
    
    product_df = product_df.withColumn(
        "discounted_price",
        when(
            trim(regexp_replace(col("discounted_price"), "[^0-9.]", "")) == "",
            lit(None)
        ).otherwise(
            regexp_replace(col("discounted_price"), "[^0-9.]", "")
        ).cast("double")
    )
    
    product_df = product_df.withColumn(
        "discount_percentage",
        when(
            trim(regexp_replace(col("discount_percentage"), "[^0-9.]", "")) == "",
            lit(None)
        ).otherwise(
            regexp_replace(col("discount_percentage"), "[^0-9.]", "")
        ).cast("double")
    )

    # ---------------------------------------
    # 3. Remove NULL / invalid values
    # ---------------------------------------
    product_df = product_df.filter(
        col("actual_price").isNotNull() &
        (col("actual_price") > 0)
    )
    
    product_df = product_df.filter(
        col("discounted_price").isNotNull() & 
        (col("discounted_price") > 0.0)
    )
    
    product_df = product_df.filter(
        col("discount_percentage").isNotNull() & 
        (col("discount_percentage") > 0.0)
    )

    return product_df