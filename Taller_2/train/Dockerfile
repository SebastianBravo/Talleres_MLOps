FROM python:3.11-slim

RUN pip install --no-cache-dir jupyterlab
COPY requirements.txt .
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--NotebookApp.token=''", \
     "--NotebookApp.password=''"]