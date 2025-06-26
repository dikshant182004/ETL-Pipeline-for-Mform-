import sys
import json
from bson import json_util
from etl.logger import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer
from etl.schemas import form_schema,language_schema,question_schema,parent_schema,child_schema,validation_schema,answer_option_schema,did_schema,coord_schema,range_rule_schema,resource_url_schema,restriction_schema,weightage_schema,get_dynamic_option_schema,data_orders_mapping_schema,projects_schema,create_dynamic_option_schema
from etl.entity.artifact_entity import ExtractionArtifact
from etl.exception import ETL_Exception

class FormTransformer:

    def __init__(self, spark: SparkSession, extract_artifact: ExtractionArtifact):
        self.spark = spark
        self.raw_json = extract_artifact.raw_json
        self.form_id = extract_artifact.form_id
        self.df = self._load_raw_data()

        self.lang_df = self.df.select(
            col("_id.$oid").alias("form_id"),
            explode("language").alias("language")
        )

    def _creating_spark_session(self):
        logging.info("Creating spark session.")
        spark = SparkSession.builder \
            .appName("Mform data") \
            .master("local[*]") \
            .getOrCreate()
        
    def _load_raw_data(self):
        logging.info("Loading raw JSON for tranformation")
        return self.spark.read.schema(form_schema).json(self.spark.sparkContext.parallelize([self.raw_json]))
    
    def transform_form_data(self):
        logging.info(" Transforming form data.")

        form_df = self.df.select(
            col("_id.$oid").alias("form_id"),
            col("formSchedule.schedules").alias("form_schedules"),
            col("project.$oid").alias("project"),
            col("projects"),
            col("isHidden").cast("boolean"),
            col("projectOrder").cast("int"),
            col("organisationId.$oid").alias("organisation_id"),
            col("editable").cast("boolean"),
            col("saveDataToCollection"),
            col("shortKey"),
            col("masterKey"),
            col("repairImportedQuestion").cast("boolean"),
            col("generateRunTimeUniqueId").cast("boolean"),
            col("location").cast("boolean"),
            col("isPreviewEnabled").cast("boolean"),
            col("reportedDateOrder").cast("int"),
            col("surveyorOrder").cast("int"),
            col("stateOrder").cast("int"),
            col("districtOrder").cast("int"),
            col("blockOrder").cast("int"),
            col("gramPanchayatOrder").cast("int"),
            col("villageOrder").cast("int"),
            col("hamletOrder").cast("int"),
            col("dynamicOrder").cast("int"),
            col("isVisibleToAll").cast("boolean"),
            col("isLivelihood").cast("boolean"),
            col("isMedia").cast("boolean"),
            col("dynamicData").cast("boolean"),
            col("expiryDate.$date").cast("timestamp").alias("expiryDate"),
            col("minAppVersion"),
            col("isActive").cast("boolean"),
            col("viewSequence"),
            col("restrictedDays"),
            col("duplicateCheckQuestions"),
            col("keyInfoOrders"),
            col("nonBypassableOrders"),
            col("modifiedAtOrder"),
            col("hideAllLabel").cast("boolean"),
            col("isDynamicCard").cast("boolean"),
            col("isMaster").cast("boolean"),
            col("hasCensus").cast("boolean"),
            col("isOnline").cast("boolean"),
            col("isDraftDisable").cast("boolean"),
            col("backgroundVoice").cast("boolean"),
            col("isDashboardDisable").cast("boolean"),
            col("isViewOnly").cast("boolean"),
            col("resetButton").cast("boolean"),
            col("cardDesign"),
            col("isResourceRepository").cast("boolean"),
            col("isCustomLabel").cast("boolean"),
            col("isBulkUploadResponse").cast("boolean"),
            col("isBulkUploadDraft").cast("boolean"),
            col("onDemandDownload").cast("boolean"),
            col("lockConfig"),
            col("formType"),
            col("analysisStatus").cast("boolean"),
            col("isReferenceData").cast("boolean"),
            col("mainFormId"),
            col("actions"),
            col("tags"),
            col("formId").cast("int"),
            col("__v").alias("form_version"),
            col("createDynamicOption"),
            col("createdAt").cast("timestamp"),
            col("externalResource"),
            col("fillCount").cast("int"),
            col("formIcon"),
            col("getDynamicOption"),
            col("hooks"),
            col("mandatoryModules"),
            col("googleSheet"),
            col("masterConfig"),
            col("modifiedAt.$date").cast("timestamp").alias("modifiedAt"),
            col("version"),
            col("maskingConfig")
        )
        return form_df
    
    def get_dynamic_option_schema_data(self):
        try:
            logging.info("Transforming getDynamicOption array...")

            dynamic_opt_df = self.df.select(
                col("_id.$oid").alias("form_id"),
                explode("getDynamicOption").alias("opt")
            )

            get_dynamic_df = dynamic_opt_df.select(
                col("form_id"),
                col("opt._id.$oid").alias("option_id"),
                col("opt.formId").cast("int"),
                col("opt.orderToDisplayIn"),
                col("opt.isReusuable").cast("boolean"),
                col("opt.isPrimary").cast("boolean"),
                col("opt.limit").cast("int"),
                col("opt.matchWithCreator").cast("boolean"),
                col("opt.filterBy"),
                col("opt.conditions"),
                col("opt.closedCreator"),
                col("opt.closedItself"),
                col("opt.showSummary")
            )

            mapping_df = dynamic_opt_df.select(
                col("form_id"),
                col("opt._id.$oid").alias("option_id"),
                explode("opt.dataOrdersMapping").alias("mapping")
            )

            mapped = mapping_df.select(
                col("option_id"),
                col("mapping._id.$oid").alias("mapping_id"),
                col("mapping.fromOrder"),
                col("mapping.toOrder")
            )

            final_dynamic_df = self.spark.createDataFrame(get_dynamic_df.rdd, schema=get_dynamic_option_schema)
            final_dynamic_data_mapping_df = self.spark.createDataFrame(mapped.rdd, schema=data_orders_mapping_schema)

            return final_dynamic_data_mapping_df,final_dynamic_df

        except Exception as e:
            raise ETL_Exception(e, sys)
        
    def transform_create_dynamic_option_data(self):
        try:
            logging.info("Transforming createDynamicOption array into flat table")

            cdo_df = self.df.select(
                col("_id.$oid").alias("form_id"),
                explode("createDynamicOption").alias("dynamic_option")
            ).select(
                col("form_id"),
                col("dynamic_option._id.$oid").alias("dynamic_option_id"),
                col("dynamic_option.childGroup").alias("child_group"),
                col("dynamic_option.optionIdentifier").alias("option_identifiers"),
                col("dynamic_option.conditions").alias("conditions"),
                col("dynamic_option.parentOrder").alias("parent_order"),
                col("dynamic_option.formId").alias("form_ref_id"),
                col("dynamic_option.order").alias("order")
            )

            return self.spark.createDataFrame(cdo_df.rdd, schema=create_dynamic_option_schema)

        except Exception as e:
            raise ETL_Exception(e, sys)

        
    def transform_projects_data(self):
        try:
            logging.info("Transforming projects array into flat table")

            projects_df = self.df.select(
                col("_id.$oid").alias("form_id"),
                explode("projects").alias("project")
            ).select(
                col("form_id"),
                col("project.$oid").alias("project_id")
            )

            return self.spark.createDataFrame(projects_df.rdd, schema=projects_schema)

        except Exception as e:
            raise ETL_Exception(e, sys)

    def transform_language_data(self):
        logging.info("Extracting language and question data")

        try:
            if "language" not in self.df.columns:
                raise ValueError("Field `language` not found in the DataFrame")

            language_df = self.lang_df.select(
                col("form_id"),
                col("language._id.$oid").alias("language_id"),
                col("language.title").alias("language_title"),
                col("language.buttons").alias("buttons")
            )

            final_language_df = self.spark.createDataFrame(language_df.rdd,schema=language_schema)

        except Exception as e :
            raise ETL_Exception(e,sys)

        return final_language_df
    
class LanguageQuestionTransformer(FormTransformer):

    def __init__(self, spark, extract_artifact):
        super().__init__(spark, extract_artifact)
        self.ques_df = self.lang_df.select(
            col("form_id"),
            col("language._id.$oid").alias("language_id"),
            explode("language.question").alias("question")
        )

    def transform_questions_data(self):

        question_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            col("language_id"),
            col("question.label"),
            col("question.title"),
            col("question.shortKey"),
            col("question.order").cast("int").alias("order"),
            col("question.viewSequence"),
            col("question.input_type"),
            col("question.editable").cast("boolean").alias("editable"),
            col("question.isOtherMaster").cast("boolean").alias("isOtherMaster"),
            col("question.isEncrypted").cast("boolean").alias("isEncrypted"),
            col("question.showComment").cast("boolean").alias("showComment"),
            col("question.information"),
            col("question.hint")
        )

        final_question_df = self.spark.createDataFrame(question_df.rdd,schema=question_schema)
        return final_question_df 
    
    def transform_questions_parent_data(self):

        parent_df = self.ques_df.select(
            col("question._id.$oid").alias("question_id"),
            explode_outer("question.parent").alias("parent_item"))
        
        new_parent_df = parent_df.select(
            col("question_id"),
            col("parent_item.value").alias("value"),
            col("parent_item.type").alias("type"),
            col("parent_item.order").cast("int").alias("order")
        )

        final_parent_df = self.spark.createDataFrame(new_parent_df.rdd,schema=parent_schema)
        return final_parent_df
    
    def transform_questions_child_data(self):

        child_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            explode_outer(col("question.child")).alias("child_item"))

        new_child_df=child_df.select(
            col("question_id"),
            col("child_item.type").alias("type"),
            col("child_item.value").alias("value"),
            col("child_item.order").cast("int").alias("order")
        )

        final_child_df = self.spark.createDataFrame(new_child_df.rdd, schema=child_schema)
        return final_child_df
    
    def transform_questions_validation_data(self):

        validation_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            explode_outer("question.validation").alias("val_item"))

        new_validation_df=validation_df.select(
            col("question_id"),
            col("val_item._id").alias("validation_id"),
            col("val_item.value").alias("value"),
            col("val_item.error_msg").alias("error_msg")
        )

        final_validation_df = self.spark.createDataFrame(new_validation_df.rdd, schema=validation_schema)
        return final_validation_df
    
    def transform_questions_answer_option_data(self):

        answer_option_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            explode_outer("question.answer_option").alias("option"))
        
        new_answer_option_df=answer_option_df.select(
            col("question_id"),
            col("option._id").alias("option_id"),
            col("option.name").alias("name"),
            col("option.shortKey").alias("shortKey"),
            col("option.viewSequence").alias("viewSequence"),
            col("option.visibility").alias("visibility")
        )

        did_df = answer_option_df.select(
            col("option._id").alias("option_id"),
            explode_outer(col("option.did")).alias("did"))
        
        coord_df = answer_option_df.select(
            col("option._id").alias("option_id"),
            explode_outer(col("option.coordinates")).alias("coordinate")
        )

        final_answer_option_df = self.spark.createDataFrame(new_answer_option_df.rdd, schema=answer_option_schema)
        final_answer_option_did_df=self.spark.createDataFrame(did_df.rdd,schema=did_schema)
        final_answer_option_coordinates_df=self.spark.createDataFrame(coord_df.rdd,schema=coord_schema)

        return final_answer_option_df,final_answer_option_coordinates_df,final_answer_option_did_df

    def transform_questions_range_rule_data(self):

        range_rule_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            col("question.min").cast("int").alias("min"),
            col("question.max").cast("int").alias("max"),
            col("question.pattern").alias("pattern")
        ).filter(
            col("min").isNotNull() | col("max").isNotNull() | col("pattern").isNotNull()
        )

        final_range_df = self.spark.createDataFrame(range_rule_df.rdd,schema=range_rule_schema)
        return final_range_df
    
    def transform_questions_remaining_data(self):

        resource_urls_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            explode_outer("question.resource_urls").alias("resource_urls"))

        restrictions_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            explode_outer("question.restrictions").alias("restriction"))

        weightage_df = self.lang_df.select(
            col("question._id.$oid").alias("question_id"),
            explode_outer("question.weightage").alias("weightage"))

        final_resource_urls_df=self.spark.createDataFrame(resource_urls_df.rdd,schema=resource_url_schema)
        final_restrictions_df=self.spark.createDataFrame(restrictions_df.rdd,schema=restriction_schema)
        final_weightage_df=self.spark.createDataFrame(weightage_df.rdd,schema=weightage_schema)

        return final_resource_urls_df,final_restrictions_df,final_weightage_df
    
