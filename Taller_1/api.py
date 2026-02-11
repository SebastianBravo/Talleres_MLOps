import os
import joblib
import pandas as pd
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field
from typing import Annotated, Literal
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

# Load the trained models
models_folder = "models"
svm_model = joblib.load(os.path.join(models_folder, "svm.pkl"))
logistic_regression_model = joblib.load(
    os.path.join(models_folder, "logistic_regression.pkl")
)
random_forest_model = joblib.load(os.path.join(models_folder, "random_forest.pkl"))

app = FastAPI()


# Data model for a penguin
class Penguin(BaseModel):
    """
    Data model for a penguin, used for input validation in the API.
    """

    island: Literal["Biscoe", "Dream", "Torgersen"] = Field(
        ...,
        description="Island where the penguin was observed. Must be one of 'Biscoe', 'Dream', 'Torgersen'.",
    )
    bill_length_mm: float = Field(
        ..., gt=0, description="Length of the penguin's bill in millimeters."
    )
    bill_depth_mm: float = Field(
        ..., gt=0, description="Depth of the penguin's bill in millimeters."
    )
    flipper_length_mm: int = Field(
        ..., gt=0, description="Length of the penguin's flipper in millimeters."
    )
    body_mass_g: float = Field(
        ..., gt=0, description="Mass of the penguin's body in grams."
    )
    sex: Literal["male", "female"] = Field(
        ..., description="Sex of the penguin. Must be 'male' or 'female'."
    )
    year: int = Field(
        ..., ge=2000, le=2024, description="Year when the penguin was observed."
    )


def preprocess_input(penguin: Penguin):
    """
    Preprocess the input penguin data for prediction.

    Args:
        penguin (Penguin): A Penguin object containing the features of the penguin.
    Returns:
        pd.DataFrame: A DataFrame containing the preprocessed features ready for prediction.
    """
    # Convert the input data to a DataFrame
    df = pd.DataFrame(
        [
            {
                "bill_length_mm": penguin.bill_length_mm,
                "bill_depth_mm": penguin.bill_depth_mm,
                "flipper_length_mm": penguin.flipper_length_mm,
                "body_mass_g": penguin.body_mass_g,
                "year": penguin.year,
                "island_Biscoe": True if penguin.island == "Biscoe" else False,
                "island_Dream": True if penguin.island == "Dream" else False,
                "island_Torgersen": True if penguin.island == "Torgersen" else False,
                "sex_female": True if penguin.sex == "female" else False,
                "sex_male": True if penguin.sex == "male" else False,
            }
        ]
    )

    return df


# Get method to predict the species of a penguin
@app.get("/predict")
def predict_species(
    penguin: Annotated[
        Penguin,
        Query(),
        Field(description="A Penguin object containing the features of the penguin."),
    ],
):
    """
    Predict the species of a penguin based on its features.

    Args:
        penguin (Penguin): A Penguin object containing the features of the penguin.
    Returns:
        str: The predicted species of the penguin.
    """
    # Create a feature vector from the input penguin data
    x = preprocess_input(penguin)

    print(x)

    # Predict the species using the svm model
    predicted_species = svm_model.predict(x)[0]

    # Map the predicted label to the actual species name
    species_mapping = {0: "Adelie", 1: "Chinstrap", 2: "Gentoo"}
    predicted_species_name = species_mapping.get(predicted_species, "Unknown")

    return {"predicted_species": predicted_species_name}
