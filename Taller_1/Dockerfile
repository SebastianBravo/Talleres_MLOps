FROM python:3.12

WORKDIR /app
COPY . /app

RUN pip3 install --no-cache-dir --upgrade -r requirements.txt


# Convert CRLF to LF and make entrypoint executable
RUN sed -i 's/\r$//' entrypoint.sh && chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]