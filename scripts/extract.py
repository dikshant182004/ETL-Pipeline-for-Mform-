import sys
import json
from pymongo import MongoClient
from bson import json_util
from etl.logger import logging
from etl.entity.config_entity import ExtractConfig
from etl.entity.artifact_entity import ExtractionArtifact
from etl.exception import ETL_Exception

extract_config = ExtractConfig()

def fetch_form(form_id=None, collection=None):
    try:
        client = MongoClient(extract_config.uri)
        logging.info("Connected to the MongoDB database")

        db = client[extract_config.database]
        if collection == "f":
            collection_data = db[extract_config.collection]
            collection_name=extract_config.collection
        elif collection == "cf":
            collection_data = db[extract_config.client_collection]
            collection_name=extract_config.client_collection
        else:
            raise ValueError(f"Invalid collection argument: {collection}")

        query = {"_id": form_id} if form_id else {}
        doc = collection_data.find_one(query)

        if not doc:
            raise ValueError("Some error")
        
        json_str=json.dumps(doc, default=json_util.default)
        artifact = ExtractionArtifact(
        form_id={"$oid": str(doc["_id"])},
        raw_json=json_str,
        source=f"{extract_config.database}-{collection_name}"
        )

        logging.info(f"Importing the reguired form with id : {form_id} for furthur transformation.")
        return artifact
    
    except Exception as e:
            raise ETL_Exception(e,sys)

