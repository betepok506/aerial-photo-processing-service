FROM python:3.9-slim
COPY search-images/search-images.py utils.py requirements.txt /

RUN python3 -m pip install --upgrade pip && \
    pip install -r requirements.txt

ENTRYPOINT ["python3", "search-images.py"]