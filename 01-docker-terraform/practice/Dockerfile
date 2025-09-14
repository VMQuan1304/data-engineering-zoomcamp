FROM python:3.9.1

# RUN apt-get install -y curl
# RUN apt-get install -y gunzip

RUN pip install pandas sqlalchemy psycopg2-binary

WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]