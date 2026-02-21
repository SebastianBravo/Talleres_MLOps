#!/bin/sh

cd app

# Run the training script to train the model and save it to disk
python train.py

# Start the FastAPI server to serve predictions
uvicorn api:app --host 0.0.0.0 --port 8989