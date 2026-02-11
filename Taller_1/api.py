import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

app = FastAPI()

# Data model for a penguin
class Penguin(BaseModel):
    """
    Data model for a penguin, used for input validation in the API.
    """
    # PONER AQUÍ LOS ATRIBUTOS NECESARIOS PARA LA PREDICCIÓN, POR EJEMPLO:
    name: str
    species: str
    age: int

# Get method to predict the species of a penguin
@app.get("/predict")
def predict_species(penguin: Penguin):
    """
    Predict the species of a penguin based on its name, age, and other features.
    
    Args:
        penguin (Penguin): A Penguin object containing the features of the penguin.
    Returns:
        str: The predicted species of the penguin.
    """

    # Load the trained model
    # POR HACER
    
    # Create a feature vector from the input penguin data
    # x = .... 
    
    # Predict the species using the model
    # POR HACER
    
    return {"predicted_species": "Adelie"}  # Placeholder return value, replace with actual prediction
