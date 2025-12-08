FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY hub_server.py hub_server.py
COPY consumer_client.html consumer_client.html

CMD ["python", "hub_server.py", "--host", "0.0.0.0", "--port", "9000"]
