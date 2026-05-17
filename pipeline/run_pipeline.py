from src.extract import extract_data
from src.transform import transform_data
from src.features import build_features
from src.load import save_to_s3
from config.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH

def run_pipeline():

    # EXTRACT
    df, spark = extract_data("data/amazon.csv")

    # TRANSFORM
    product_df = transform_data(df)

    # SAVE BRONZE
    save_to_s3(product_df, BRONZE_PATH)

    # NORMALIZE
    # products_df, users_df, df_reviews = normalize_data(df)

    # FEATURES
    df_features = build_features(product_df)

    # SAVE SILVER
    # save_to_s3(products_df, SILVER_PATH + "products/")
    # save_to_s3(users_df, SILVER_PATH + "users/")
    # save_to_s3(df_features, SILVER_PATH + "reviews/")

    # FINAL GOLD LAYER
    save_to_s3(df_features, GOLD_PATH)

    spark.stop()

if __name__ == "__main__":
    run_pipeline()