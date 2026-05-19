from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator

NUMERIC_COLUMNS = [
    "time_in_hospital", "num_lab_procedures", "num_procedures", "num_medications",
    "number_outpatient", "number_emergency", "number_inpatient", "number_diagnoses",
]

CATEGORICAL_COLUMNS = [
    "race", "gender", "age", "weight",
    "admission_type_id", "discharge_disposition_id", "admission_source_id",
    "payer_code", "medical_specialty",
    "diag_1", "diag_2", "diag_3",
    "max_glu_serum", "a1cresult",
    "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride",
    "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone",
    "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide",
    "examide", "citoglipton", "insulin",
    "glyburide-metformin", "glipizide-metformin", "glimepiride-pioglitazone",
    "metformin-rosiglitazone", "metformin-pioglitazone",
    "change", "diabetesmed",
]

FEATURE_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS


class PredictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    time_in_hospital: Optional[float] = None
    num_lab_procedures: Optional[float] = None
    num_procedures: Optional[float] = None
    num_medications: Optional[float] = None
    number_outpatient: Optional[float] = None
    number_emergency: Optional[float] = None
    number_inpatient: Optional[float] = None
    number_diagnoses: Optional[float] = None

    race: Optional[str] = None
    gender: Optional[str] = None
    age: Optional[str] = None
    weight: Optional[str] = None
    admission_type_id: Optional[int] = None
    discharge_disposition_id: Optional[int] = None
    admission_source_id: Optional[int] = None
    payer_code: Optional[str] = None
    medical_specialty: Optional[str] = None
    diag_1: Optional[str] = None
    diag_2: Optional[str] = None
    diag_3: Optional[str] = None
    max_glu_serum: Optional[str] = None
    a1cresult: Optional[str] = None
    metformin: Optional[str] = None
    repaglinide: Optional[str] = None
    nateglinide: Optional[str] = None
    chlorpropamide: Optional[str] = None
    glimepiride: Optional[str] = None
    acetohexamide: Optional[str] = None
    glipizide: Optional[str] = None
    glyburide: Optional[str] = None
    tolbutamide: Optional[str] = None
    pioglitazone: Optional[str] = None
    rosiglitazone: Optional[str] = None
    acarbose: Optional[str] = None
    miglitol: Optional[str] = None
    troglitazone: Optional[str] = None
    tolazamide: Optional[str] = None
    examide: Optional[str] = None
    citoglipton: Optional[str] = None
    insulin: Optional[str] = None
    glyburide_metformin: Optional[str] = None
    glipizide_metformin: Optional[str] = None
    glimepiride_pioglitazone: Optional[str] = None
    metformin_rosiglitazone: Optional[str] = None
    metformin_pioglitazone: Optional[str] = None
    change: Optional[str] = None
    diabetesmed: Optional[str] = None

    @field_validator("*", mode="before")
    @classmethod
    def empty_string_to_none(cls, value: Any):
        return None if value == "" else value

    def to_dataframe(self) -> pd.DataFrame:
        data = self.model_dump()
        rename_map = {
            "glyburide_metformin":        "glyburide-metformin",
            "glipizide_metformin":        "glipizide-metformin",
            "glimepiride_pioglitazone":   "glimepiride-pioglitazone",
            "metformin_rosiglitazone":    "metformin-rosiglitazone",
            "metformin_pioglitazone":     "metformin-pioglitazone",
        }
        data = {rename_map.get(k, k): v for k, v in data.items()}
        row = {col: data.get(col) for col in FEATURE_COLUMNS}
        return pd.DataFrame([row], columns=FEATURE_COLUMNS)
