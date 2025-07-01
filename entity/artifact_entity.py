from dataclasses import dataclass
from pyspark.sql import DataFrame

@dataclass
class ExtractionArtifact:
    form_id: str
    raw_json: str
    source: str  

@dataclass
class TransformationArtifact:
    form_df: DataFrame
    language_df: DataFrame
    question_df: DataFrame
    parent_df: DataFrame
    child_df: DataFrame
    validation_df: DataFrame
    answer_option_df: DataFrame
    range_rule_df: DataFrame
    restriction_df: DataFrame
    resource_url_df: DataFrame
    weightage_df: DataFrame
    restriction_order_df: DataFrame
    get_dynamic_option_df: DataFrame
    get_dynamic_option_mapping_df: DataFrame
    create_dynamic_option_df: DataFrame
    projects_df: DataFrame

@dataclass
class ClientTransformationArtifact:
    clientform_df: DataFrame
    question_df: DataFrame
    initial_answer_df: DataFrame
    answer_df: DataFrame
    reference_df: DataFrame
    location_df: DataFrame

@dataclass
class LoadArtifact:
    destination_table: str
    rows_written: int
    status: str
    form_id: str
