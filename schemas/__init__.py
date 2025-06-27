from pyspark.sql.types import *


form_schema = StructType([
    StructField("_id", StructType([StructField("$oid", StringType())])),
    StructField("formSchedule", StructType([
        StructField("schedules", ArrayType(StringType()))
    ])),
    StructField("project", StructType([StructField("$oid", StringType())])),
    StructField("projects", ArrayType(
    StructType([
        StructField("$oid", StringType())
    ])
)),
    StructField("isHidden", BooleanType()),
    StructField("projectOrder", IntegerType()),
    StructField("organisationId", StructType([StructField("$oid", StringType())])),
    StructField("editable", BooleanType()),
    StructField("saveDataToCollection", StringType()),
    StructField("shortKey", StringType()),
    StructField("masterKey", StringType()),
    StructField("repairImportedQuestion", BooleanType()),
    StructField("generateRunTimeUniqueId", BooleanType()),
    StructField("location", BooleanType()),
    StructField("isPreviewEnabled", BooleanType()),
    StructField("reportedDateOrder", IntegerType()),
    StructField("surveyorOrder", IntegerType()),
    StructField("stateOrder", IntegerType()),
    StructField("districtOrder", IntegerType()),
    StructField("blockOrder", IntegerType()),
    StructField("gramPanchayatOrder", IntegerType()),
    StructField("villageOrder", IntegerType()),
    StructField("hamletOrder", IntegerType()),
    StructField("dynamicOrder", IntegerType()),
    StructField("isVisibleToAll", BooleanType()),
    StructField("isLivelihood", BooleanType()),
    StructField("isMedia", BooleanType()),
    StructField("dynamicData", BooleanType()),
    StructField("expiryDate", StructType([StructField("$date", StringType())])),
    StructField("minAppVersion", StringType()),
    StructField("isActive", BooleanType()),
    StructField("viewSequence", StringType()),
    StructField("restrictedDays", ArrayType(StringType())),
    StructField("duplicateCheckQuestions", ArrayType(StringType())),
    StructField("keyInfoOrders", ArrayType(StringType())),
    StructField("nonBypassableOrders", ArrayType(StringType())),
    StructField("modifiedAtOrder", ArrayType(StringType())),
    StructField("hideAllLabel", BooleanType()),
    StructField("isDynamicCard", BooleanType()),
    StructField("isMaster", BooleanType()),
    StructField("hasCensus", BooleanType()),
    StructField("isOnline", BooleanType()),
    StructField("isDraftDisable", BooleanType()),
    StructField("backgroundVoice", BooleanType()),
    StructField("isDashboardDisable", BooleanType()),
    StructField("isViewOnly", BooleanType()),
    StructField("resetButton", BooleanType()),
    StructField("cardDesign", StringType()),
    StructField("isResourceRepository", BooleanType()),
    StructField("isCustomLabel", BooleanType()),
    StructField("isBulkUploadResponse", BooleanType()),
    StructField("isBulkUploadDraft", BooleanType()),
    StructField("onDemandDownload", BooleanType()),
    StructField("lockConfig", ArrayType(StringType())),
    StructField("formType", StringType()),
    StructField("analysisStatus", BooleanType()),
    StructField("isReferenceData", BooleanType()),
    StructField("mainFormId", StringType()),
    StructField("actions", ArrayType(StringType())),
    StructField("tags", ArrayType(StringType())),
    StructField("formId", IntegerType()),
    StructField("__v", StringType()),
    StructField("createDynamicOption", ArrayType(
    StructType([
        StructField("childGroup", StringType(), nullable=True),
        StructField("optionIdentifier", ArrayType(StringType())),
        StructField("conditions", ArrayType(StringType())),  
        StructField("_id", StructType([StructField("$oid", StringType())])),
        StructField("parentOrder", StringType()),
        StructField("formId", IntegerType()),
        StructField("order", StringType())
    ])
)),
    StructField("createdAt", TimestampType()),
    StructField("externalResource", ArrayType(StringType())),
    StructField("fillCount", IntegerType()),
    StructField("formIcon", StringType()),
    StructField("getDynamicOption", ArrayType(
    StructType([
        StructField("_id", StructType([StructField("$oid", StringType())])),
        StructField("formId", IntegerType()),
        StructField("orderToDisplayIn", StringType()),
        StructField("isReusuable", BooleanType()),
        StructField("isPrimary", BooleanType()),
        StructField("limit", IntegerType()),
        StructField("matchWithCreator", BooleanType()),
        StructField("filterBy", ArrayType(StringType())),
        StructField("conditions", ArrayType(StringType())),
        StructField("closedCreator", ArrayType(StringType())),
        StructField("closedItself", ArrayType(StringType())),
        StructField("showSummary", ArrayType(StringType())),
        StructField("dataOrdersMapping", ArrayType(
            StructType([
                StructField("_id", StructType([StructField("$oid", StringType())])),
                StructField("fromOrder", StringType()),
                StructField("toOrder", StringType())
            ])
        ))
    ])
)),
    StructField("hooks", ArrayType(StringType())),
    StructField("mandatoryModules", ArrayType(StringType())),
    StructField("googleSheet", MapType(StringType(), StringType())),
    StructField("masterConfig", ArrayType(StringType())),
    StructField("language", ArrayType(
    StructType([
        StructField("_id", StructType([StructField("$oid", StringType())])),
        StructField("title", StringType()),
        StructField("buttons", ArrayType(StringType())),
        StructField("question", ArrayType(StructType([
            StructField("_id", StructType([
                StructField("$oid", StringType())
            ])),
            StructField("label", StringType()),
            StructField("title", StringType()),
            StructField("shortKey", StringType()),
            StructField("order", StringType()),
            StructField("viewSequence", StringType()),
            StructField("input_type", StringType()),
            StructField("editable", BooleanType()),
            StructField("isOtherMaster", BooleanType()),
            StructField("isEncrypted", BooleanType()),
            StructField("showComment", BooleanType()),
            StructField("information", StringType()),
            StructField("hint", StringType()),
            StructField("min", IntegerType()),
            StructField("max", IntegerType()),
            StructField("pattern", StringType()),

            StructField("parent", ArrayType(StructType([
                StructField("value", StringType()),
                StructField("type", StringType()),
                StructField("order", StringType())
            ]))),

            StructField("child", ArrayType(StructType([
                StructField("value", StringType()),
                StructField("type", StringType()),
                StructField("order", StringType())
            ]))),

            StructField("validation", ArrayType(StructType([
                StructField("_id", StringType()),
                StructField("value", StringType()),
                StructField("error_msg", StringType())
            ]))),

            StructField("answer_option", ArrayType(StructType([
                StructField("_id", StructType([
                    StructField("$oid", StringType())
                ])),
                StructField("name", StringType()),
                StructField("shortKey", StringType()),
                StructField("viewSequence", StringType()),
                StructField("visibility", StringType()),

                StructField("did", StringType()),
                StructField("coordinate", StringType())
            ]))),

            StructField("resource_urls", ArrayType(StringType())),

            StructField("restrictions", ArrayType(StructType([
                StructField("type", StringType()),
                StructField("value", StringType())
            ]))),
            StructField("weightage", ArrayType(StructType([
                StructField("type", StringType()),
                StructField("value", StringType())
            ]))),

            ])))
        ])
    )),
    StructField("modifiedAt", StructType([StructField("$date", StringType())])),
    StructField("version", StringType()),
    StructField("maskingConfig", ArrayType(StringType())),
])

""" structured schemas for createDynamicOption data"""

create_dynamic_option_schema = StructType([
    StructField("form_id", StringType()),  # FK from parent form table
    StructField("dynamic_option_id", StringType()),  
    StructField("child_group", StringType(), nullable=True),
    StructField("option_identifiers", ArrayType(StringType())),
    StructField("conditions", ArrayType(StringType())),  
    StructField("parent_order", StringType()),
    StructField("form_ref_id", IntegerType()),  # from createDynamicOption.formId
    StructField("order", StringType())
])

""" structured schemas for getDynamicOption data"""

get_dynamic_option_schema = StructType([
    StructField("form_id", StringType()),                # FK from parent
    StructField("option_id", StringType()),
    StructField("formId", IntegerType()),
    StructField("orderToDisplayIn", StringType()),
    StructField("isReusuable", BooleanType()),
    StructField("isPrimary", BooleanType()),
    StructField("limit", IntegerType()),
    StructField("matchWithCreator", BooleanType()),
    StructField("filterBy", ArrayType(StringType())),
    StructField("conditions", ArrayType(StringType())),
    StructField("closedCreator", ArrayType(StringType())),
    StructField("closedItself", ArrayType(StringType())),
    StructField("showSummary", ArrayType(StringType()))
])

data_orders_mapping_schema = StructType([
    StructField("option_id", StringType()),      # FK from getDynamicOption._id
    StructField("mapping_id", StringType()),     # _id.$oid
    StructField("fromOrder", StringType()),
    StructField("toOrder", StringType())
])

""" structured schemas for projects data """
projects_schema = StructType([
    StructField("form_id", StringType()),        # FK from form
    StructField("project_id", StringType())     
])

""" structured schemas for language data """
language_schema = StructType([
    StructField("form_id", StringType()),  # FK from form table
    StructField("language_id", StringType()), 
    StructField("language_title", StringType()),
    StructField("buttons", ArrayType(StringType()), nullable=True)  
])

###  FOR QUESTIONS DATA 

question_schema = StructType([
    StructField("question_id", StringType(), True),       # question._id.$oid
    StructField("language_id", StringType(), True),       # language._id.$oid
    StructField("label", StringType(), True),
    StructField("title", StringType(), True),
    StructField("shortKey", StringType(), True),
    StructField("order", IntegerType(), True),
    StructField("viewSequence", StringType(), True),
    StructField("input_type", StringType(), True),
    StructField("editable", BooleanType(), True),
    StructField("isOtherMaster", BooleanType(), True),
    StructField("isEncrypted", BooleanType(), True),
    StructField("showComment", BooleanType(), True),
    StructField("information", StringType(), True),
    StructField("hint", StringType(), True)
])

parent_schema = StructType([
    StructField("question_id", StringType(), True),  # FK to questions table
    StructField("value", StringType(), True),
    StructField("type", StringType(), True),
    StructField("order", IntegerType(), True)
])

child_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("value", StringType(), True),
    StructField("order", IntegerType(), True)
])

validation_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("validation_id", StringType(), True),
    StructField("value", StringType(), True),
    StructField("error_msg", StringType(), True)
])

answer_option_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("option_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("shortKey", StringType(), True),
    StructField("viewSequence", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("did", StringType(), True),
    StructField("coordinate", StringType(), True)
])

range_rule_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("min", IntegerType(), True),
    StructField("max", IntegerType(), True),
    StructField("pattern", StringType(), True)
])

resource_url_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("resource_urls", StringType(), True)
])

restriction_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("restriction", MapType(StringType(), StringType()), True)  
])

weightage_schema = StructType([
    StructField("question_id", StringType(), True),
    StructField("weightage", MapType(StringType(), StringType()), True)  
])