import argparse
from bson import ObjectId
from pyspark.sql import SparkSession
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from etl.scripts.extract import fetch_form
from etl.scripts.transform import LanguageQuestionTransformer
from etl.scripts.transform_clientformdata import ClientFormTransformer
from etl.scripts.load import PostgresLoader

console = Console()

def create_spark_session():
    return SparkSession.builder \
        .appName("Mform ETL") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.jars", "file:///J:/project/etl/libs/postgresql-42.7.3.jar") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.native.lib", "false") \
        .getOrCreate()

def run_etl_pipeline(form_id: str, collection: str):

    try:

        spark = create_spark_session()  
        spark.sparkContext.setLogLevel("ERROR")

        with console.status("[bold green]Extracting form data...", spinner="dots"):
            artifact = fetch_form(ObjectId(form_id), collection)
        console.print("[bold green]=>> Extraction complete!")
        
        """
            f --> forms collection , cf --> clientformdata collection
        """

        if collection == "f":
            with console.status("[bold green]Transforming form data...", spinner="dots"):
                transformer = LanguageQuestionTransformer(spark, artifact)
                transformed_artifact = transformer.transform_all()
            console.print("[bold green]=>> Form Transformation complete!")

            with console.status("[bold green]Loading data to PostgreSQL...", spinner="dots"):
                loader = PostgresLoader(
                    db_user="postgres",
                    db_password="Dik182004@#",
                    db_host="localhost",
                    db_port="5432",
                    db_name="FormDB"
                )
                loader.load_all(transformed_artifact, form_id)
         
        elif collection == "cf":
            with console.status("[bold green]Transforming client form data...", spinner="dots"):
                transformer =  ClientFormTransformer(spark, artifact)
                transformed_artifact = transformer.transform_all()
            console.print("[bold green]=>> Client Form Transformation complete!")

            with console.status("[bold green]Loading data to PostgreSQL...", spinner="dots"):
                loader = PostgresLoader(
                    db_user="postgres",
                    db_password="Dik182004@#",
                    db_host="localhost",
                    db_port="5432",
                    db_name="ClientFormDB"
                )
                loader.load_client_all(transformed_artifact, form_id)
        console.print("[bold green]=>> Loading complete!")
        print("\n")
        console.print("\n[bold green]:white_check_mark: ETL pipeline completed successfully!!!")
        print("\n")

    except Exception as e:
        console.print(f"[bold red]❌ Error: {e}")
    
def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline for a given form.")
    parser.add_argument("--collection", required=True, help="Collection type: forms(f) or clientformdatas(cf)")
    parser.add_argument("--form_id", required=True, help="MongoDB form ObjectId")

    args = parser.parse_args()
    run_etl_pipeline(args.form_id, args.collection)

if __name__ == "__main__":
    main()
