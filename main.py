import argparse
from bson import ObjectId
from pyspark.sql import SparkSession

from etl.scripts.extract import fetch_form
from etl.scripts.transform import LanguageQuestionTransformer
from etl.scripts.load import PostgresLoader

def create_spark_session():
    return SparkSession.builder \
        .appName("Mform ETL") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.native.lib", "false") \
        .getOrCreate()

def run_etl_pipeline(form_id_str: str):
    spark = create_spark_session()
    form_id = ObjectId(form_id_str)

    # Step 1: Extract
    artifact = fetch_form(form_id)

    # Step 2: Transform
    transformer = LanguageQuestionTransformer(spark, artifact)
    transformed_artifact = transformer.transform_all()

    # Step 3: Load
    loader = PostgresLoader()
    loader.load_all(transformed_artifact)

    print(f"ETL completed for form: {form_id_str}")

def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline for a given form.")
    parser.add_argument("--form_id", required=True, help="MongoDB form ObjectId")

    args = parser.parse_args()
    run_etl_pipeline(args.form_id)

if __name__ == "__main__":
    main()
