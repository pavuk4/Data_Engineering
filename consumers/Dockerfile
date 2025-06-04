FROM python:3.11-slim

WORKDIR /app

COPY consumer-1.py .
COPY consumer-2.py .
RUN pip install kafka-python

CMD ["python", "consumer-1.py"]
