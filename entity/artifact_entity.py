from dataclasses import dataclass
from typing import Optional, List

@dataclass
class ExtractionArtifact:
    form_id: str
    raw_json: str
    source: str  

@dataclass
class TransformationArtifact:
    row_count: int
    columns: List[str]
    status: str
    form_id: str

@dataclass
class LoadArtifact:
    destination_table: str
    rows_written: int
    status: str
    form_id: str
