import os
from datetime import datetime
from dataclasses import dataclass
from typing import Optional

from etl.constants import *

TIMESTAMP: str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

@dataclass
class ETLPipelineConfig:
    pipeline_name: str = PIPELINE_NAME
    artifact_dir: str = os.path.join(ARTIFACT_DIR, TIMESTAMP)
    timestamp: str = TIMESTAMP

etl_pipeline_config = ETLPipelineConfig()

@dataclass
class ExtractConfig:
    uri: str = MONGODB_URI
    database: str = DATABASE_NAME
    collection: str = COLLECTION_NAME
    client_collection: str = CLIENT_COLLECTION_NAME


@dataclass
class TransformConfig:
    form_type: Optional[str] = None

@dataclass
class LoadConfig:
    pass
