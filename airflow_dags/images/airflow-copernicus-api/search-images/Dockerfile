FROM python:3.9-slim
COPY data_generation.py requirements.txt /

RUN python3 -m pip install --upgrade pip && \
    pip install -r requirements.txt

ENTRYPOINT ["python3", "data_generation.py"]