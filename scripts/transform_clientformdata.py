import sys
from etl.logger import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, to_timestamp
from etl.entity.artifact_entity import ClientTransformationArtifact
from etl.schemas.clientform_schema import *
from etl.entity.artifact_entity import ExtractionArtifact
from etl.exception import ETL_Exception

class ClientFormTransformer:

    def __init__(self, spark: SparkSession, extract_artifact: ExtractionArtifact):
        self.spark = spark
        self.raw_json = extract_artifact.raw_json
        self.form_id = extract_artifact.form_id
        self.df = self._load_raw_data()
        self.question_df = None

    def _load_raw_data(self):
        logging.info("Loading raw JSON for tranformation")
        return self.spark.read.schema(submission_schema).json(self.spark.sparkContext.parallelize([self.raw_json]))
        
    def transform_form_data(self) -> DataFrame:
        try:
            logging.info("Transforming the clientform data ")

            submission_df = self.df.select(
                col("appVersion"),
                col("formId"),
                col("formUiniqueId.$oid").alias("formUniqueId"),
                col("language"),
                to_timestamp(col("mobileCreatedAt.$date")).alias("mobileCreatedAt"),
                to_timestamp(col("mobileUpdatedAt.$date")).alias("mobileUpdatedAt"),
                to_timestamp(col("reportDate.$date")).alias("reportDate"),
                col("transactionId"),
                col("version"),
                col("deviceId.$oid").alias("deviceId"),
                col("uniqueId"),
                col("isActive"),
                to_timestamp(col("createdAt.$date")).alias("createdAt"),
                to_timestamp(col("modifiedAt.$date")).alias("modifiedAt"),
                to_timestamp(col("surveyStartAt.$date")).alias("surveyStartAt"),
                col("timeTaken"),
                col("backgroundVoice"),
                col("state.$oid").alias("state"),
                col("district.$oid").alias("district"),
                col("block.$oid").alias("block"),
                col("gramPanchayat.$oid").alias("gramPanchayat"),
                col("village"),
                col("hamlet"),
                col("loginId.$oid").alias("loginId"),
                col("organisationId.$oid").alias("organisationId"),
                col("project.$oid").alias("project"),
                col("partner.$oid").alias("partner"),
                col("groupResponseId.$oid").alias("groupResponseId"),
                col("parentResponseId.$oid").alias("parentResponseId"),
                col("__v")
            )
            return submission_df
        
        except Exception as e:
            raise ETL_Exception(e,sys)
        
    def transform_reference_data(self) -> DataFrame:
        try:
            logging.info("Transforming the reference data.")
            reference_exploded = self.df.select(
                col("formUiniqueId.$oid").alias("formUniqueId"),
                explode(col("references")).alias("reference")
            )

            submission_reference_df = reference_exploded.select(
                col("formUniqueId"),
                col("reference.responseId.$oid").alias("responseId"),
                col("reference.formId")
            )

            final_reference_df = self.spark.createDataFrame(submission_reference_df.rdd, schema = submission_reference_schema)
            return final_reference_df
        
        except Exception as e:
            raise ETL_Exception(e,sys)
        
    def transform_location_data(self) -> DataFrame:
        try:
            logging.info("Transforming the location data.")
            submission_location_df = self.df.select(
                col("formUiniqueId.$oid").alias("formUniqueId"),
                col("location.accuracy").alias("accuracy"),
                col("location.lat").alias("latitude"),
                col("location.lng").alias("longitude")
            )

            final_location_df = self.spark.createDataFrame(submission_location_df.rdd, schema = submission_location_schema)
            return final_location_df
        
        except Exception as e:
            raise ETL_Exception(e,sys)
        
    def transform_question_data(self) -> DataFrame:
        try:
            logging.info("Transforming the questions data.")
            self.question_df = self.df.select(
                col("formUiniqueId.$oid").alias("formUniqueId"),
                explode(col("question")).alias("question")
            )

            submission_question_df = self.question_df.select(
                col("formUniqueId"),
                col("question._id.$oid").alias("question_id"),
                col("question.input_type"),
                col("question.order"),
                col("question.nestedAnswer"),
                col("question.history")
            )

            final_question_df = self.spark.createDataFrame(submission_question_df.rdd, schema = submission_question_schema)
            return final_question_df
        
        except Exception as e:
            raise ETL_Exception(e,sys)
    
    def transform_question_answer_data(self) -> DataFrame:
        try:
            logging.info("Transforming the questions -> answer  data.")
            answer_exploded = self.question_df.select(
                col("question._id.$oid").alias("question_id"),
                explode(col("question.answer")).alias("answer")
            )

            submission_answer_df = answer_exploded.select(
                col("question_id"),
                col("answer._id.$oid").alias("answer_id"),
                col("answer.label"),
                col("answer.textValue"),
                col("answer.value")
            )

            final_question_answer_df = self.spark.createDataFrame(submission_answer_df.rdd, schema = submission_answer_schema)
            return final_question_answer_df

        except Exception as e:
            raise ETL_Exception(e,sys)
        
    def transform_question_initial_answer_data(self) -> DataFrame:
        try:
            logging.info("Transforming the questions -> intital answer  data.")
            initial_answer_exploded = self.question_df.select(
                col("question._id.$oid").alias("question_id"),
                explode(col("question.intialAnswer")).alias("intialAnswer")
            )

            submission_intial_answer_df = initial_answer_exploded.select(
                col("question_id"),
                col("intialAnswer._id.$oid").alias("answer_id"),
                col("intialAnswer.label"),
                col("intialAnswer.textValue"),
                col("intialAnswer.value")
            )

            final_question_intial_answer_df = self.spark.createDataFrame(submission_intial_answer_df.rdd, schema = submission_intial_answer_schema)
            return final_question_intial_answer_df

        except Exception as e:
            raise ETL_Exception(e,sys)
        
    def transform_all(self) -> ClientTransformationArtifact:
        
        return ClientTransformationArtifact(
            clientform_df = self.transform_form_data(),
            question_df=self.transform_question_data(),
            initial_answer_df=self.transform_question_initial_answer_data(),
            answer_df=self.transform_question_answer_data(),
            reference_df=self.transform_reference_data(),
            location_df=self.transform_location_data()
        )
