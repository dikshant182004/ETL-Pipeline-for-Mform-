import os
from bson import ObjectId
from pyspark.sql import SparkSession
from etl.scripts.extract import fetch_form
from etl.scripts.transform import FormTransformer,LanguageQuestionTransformer

spark = spark = SparkSession.builder \
    .appName("Mform data") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.native.lib", "false") \
    .getOrCreate()

sample_form_id = ObjectId("6238549e6912733f1c15079f")  
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)


artifact = fetch_form(sample_form_id)
transformer = FormTransformer(spark, artifact)
transformer1 = LanguageQuestionTransformer(spark, artifact)


df1 ,df2,df3= transformer1.transform_questions_remaining_data()
df1.show(100, truncate=False)




