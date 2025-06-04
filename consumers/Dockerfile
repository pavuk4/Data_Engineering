FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer-1.py .
COPY consumer-2.py .
RUN pip install kafka-python

CMD ["python", "-u", "consumer-1.py"]
