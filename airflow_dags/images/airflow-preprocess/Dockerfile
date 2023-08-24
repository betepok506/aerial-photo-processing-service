FROM python:3.9-slim

COPY preprocess.py requirements.txt /

RUN python3 -m pip install --upgrade pip && \
    pip install -r requirements.txt

ENTRYPOINT ["python", "preprocess.py"]