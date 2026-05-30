from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict

# Column groups must match what preprocess.py fitted the ColumnTransformer on.
# The ColumnTransformer selects columns by name, so order within each group
# does not matter — but all columns must be present in the DataFrame.
NUMERIC_FEATURES   = ["bed", "bath", "acre_lot", "house_size"]
DATE_FEATURES      = ["prev_sold_year", "prev_sold_month", "prev_sold_days_ago"]
HIGH_CARD_FEATURES = ["brokered_by", "street", "city", "state", "zip_code"]
LOW_CARD_FEATURES  = ["status"]
PIPELINE_COLUMNS   = NUMERIC_FEATURES + DATE_FEATURES + HIGH_CARD_FEATURES + LOW_CARD_FEATURES

# Must match the reference date used in preprocess.py → _parse_date_features
_DATE_REFERENCE = pd.Timestamp("2025-01-01")


def _parse_prev_sold_date(date_str: Optional[str]) -> tuple:
    """Returns (prev_sold_year, prev_sold_month, prev_sold_days_ago) as floats.
    Returns (nan, nan, nan) when date_str is None or unparseable."""
    parsed = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(parsed):
        return np.nan, np.nan, np.nan
    return (
        float(parsed.year),
        float(parsed.month),
        float((_DATE_REFERENCE - parsed).days),
    )


class PropertyFeatures(BaseModel):
    """Raw property features as they arrive from the client.

    prev_sold_date is accepted as a string (YYYY-MM-DD) and parsed internally
    into the three derived date features the ColumnTransformer expects.
    zip_code is treated as a categorical string (high-cardinality OrdinalEncoder).
    """

    model_config = ConfigDict(extra="forbid")

    # Numeric features
    bed: Optional[float] = None
    bath: Optional[float] = None
    acre_lot: Optional[float] = None
    house_size: Optional[float] = None

    # Date feature — raw string, parsed by to_dataframe()
    prev_sold_date: Optional[str] = None

    # High-cardinality categoricals
    brokered_by: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None

    # Low-cardinality categorical
    status: Optional[str] = None

    def to_dataframe(self) -> pd.DataFrame:
        """Builds the DataFrame the sklearn Pipeline expects.

        Parses prev_sold_date → prev_sold_year / prev_sold_month / prev_sold_days_ago
        using the same reference date as the training preprocessor.
        """
        year, month, days_ago = _parse_prev_sold_date(self.prev_sold_date)
        row = {
            "bed": self.bed,
            "bath": self.bath,
            "acre_lot": self.acre_lot,
            "house_size": self.house_size,
            "prev_sold_year": year,
            "prev_sold_month": month,
            "prev_sold_days_ago": days_ago,
            "brokered_by": self.brokered_by,
            "street": self.street,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "status": self.status,
        }
        return pd.DataFrame([row], columns=PIPELINE_COLUMNS)


PredictRequest = PropertyFeatures
