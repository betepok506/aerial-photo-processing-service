FROM python:3.9-slim
COPY download-images/download-images.py utils.py download-images/requirements.txt /

RUN python3 -m pip install --upgrade pip && \
    pip install -r requirements.txt

ENTRYPOINT ["python3", "download-images.py"]