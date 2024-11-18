FROM python:3.8-slim

WORKDIR /app

COPY consumer.py consumer.py

CMD ["python","consumer.py","-r","us-east-1","-rq","https://sqs.us-east-1.amazonaws.com/351775707843/cs5250-requests", "-dwt", "widgets"]
