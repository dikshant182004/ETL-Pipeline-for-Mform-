from pyspark.sql.types import *


submission_schema = StructType([
    StructField("appVersion", StringType()),
    StructField("formId", IntegerType()),
    StructField("formUiniqueId", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("language", StringType()),
    StructField("mobileCreatedAt", StructType([
        StructField("$date", TimestampType())
    ])),
    StructField("mobileUpdatedAt", StructType([
        StructField("$date", TimestampType())
    ])),
    StructField("reportDate", StructType([
        StructField("$date", TimestampType())
    ])),
    StructField("transactionId", StringType()),
    StructField("version", StringType()),
    StructField("deviceId", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("uniqueId", StringType()),
    StructField("isActive", BooleanType()),
    StructField("createdAt", StructType([
        StructField("$date", TimestampType())
    ])),
    StructField("modifiedAt", StructType([
        StructField("$date", TimestampType())
    ])),
    StructField("surveyStartAt", StructType([
        StructField("$date", TimestampType())
    ])),
    StructField("timeTaken", IntegerType()),

    StructField("question", ArrayType(
        StructType([
            StructField("input_type", StringType()),
            StructField("order", StringType()),
            StructField("_id", StructType([
                StructField("$oid", StringType())
            ])),
            StructField("nestedAnswer", ArrayType(StringType())),
            StructField("history", ArrayType(StringType())),
            StructField("intialAnswer", ArrayType(
                StructType([
                    StructField("label", StringType()),
                    StructField("textValue", StringType()),
                    StructField("value", StringType()),
                    StructField("_id", StructType([
                        StructField("$oid", StringType())
                    ]))
                ])
            )),
            StructField("answer", ArrayType(
                StructType([
                    StructField("label", StringType()),
                    StructField("textValue", StringType()),
                    StructField("value", StringType()),
                    StructField("_id", StructType([
                        StructField("$oid", StringType())
                    ]))
                ])
            ))
        ])
    )),

    StructField("backgroundVoice", StringType()),

    StructField("references", ArrayType(
        StructType([
            StructField("responseId", StructType([
                StructField("$oid", StringType())
            ])),
            StructField("formId", IntegerType())
        ])
    )),

    StructField("location", StructType([
        StructField("accuracy", StringType()),
        StructField("lat", StringType()),
        StructField("lng", StringType())
    ])),

    StructField("state", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("district", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("block", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("gramPanchayat", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("village", StringType()),
    StructField("hamlet", StringType()),

    StructField("loginId", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("organisationId", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("project", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("partner", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("groupResponseId", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("parentResponseId", StructType([
        StructField("$oid", StringType())
    ])),
    StructField("__v", IntegerType())
])


submission_question_schema = StructType([
    StructField("submission_id", StringType()),     # FK from parent (_id.$oid)
    StructField("question_id", StringType()),       # question._id.$oid
    StructField("input_type", StringType()),
    StructField("order", StringType()),
    StructField("nestedAnswer", ArrayType(StringType())),  # raw or JSON list
    StructField("history", ArrayType(StringType()))
])


submission_intial_answer_schema = StructType([
    StructField("question_id", StringType()),     # FK to question
    StructField("answer_id", StringType()),       # _id.$oid
    StructField("label", StringType()),
    StructField("textValue", StringType()),
    StructField("value", StringType())
])

submission_answer_schema = StructType([
    StructField("question_id", StringType()),     # FK
    StructField("answer_id", StringType()),       # _id.$oid
    StructField("label", StringType()),
    StructField("textValue", StringType()),
    StructField("value", StringType())
])

submission_reference_schema = StructType([
    StructField("submission_id", StringType()),  # FK
    StructField("responseId", StringType()),
    StructField("formId", IntegerType())
])

submission_location_schema = StructType([
    StructField("submission_id", StringType()),  # FK
    StructField("accuracy", StringType()),
    StructField("lat", StringType()),
    StructField("lng", StringType())
])

