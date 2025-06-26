# constants.py
import os

PIPELINE_NAME: str = "etl_pipeline"
ARTIFACT_DIR: str = "artifact"

MONGODB_URI: str = "mongodb://dikshant_mgrantv2:Bz7qP!R4vMuYfD2xWp@13.202.188.70:20585,65.1.243.251:20585,3.111.140.243:20585/mgrant-staging-v2?replicaSet=rs0&authSource=admin"
DATABASE_NAME: str = "mgrant-staging-v2"
COLLECTION_NAME: str = "forms"

"""Constants for data Extraction process."""

DATA_EXTRACTION_COLLECTION_NAME: str = "form_data"
DATA_EXTRACTION_DIR_NAME: str = "extract"