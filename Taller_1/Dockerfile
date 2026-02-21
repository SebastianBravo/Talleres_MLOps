FROM python:3.12-slim

WORKDIR /app
COPY . /app

RUN pip3 install --no-cache-dir --upgrade -r requirements.txt

RUN python app/train.py

CMD [ "uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8989" ]