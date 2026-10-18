FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn pydantic kafka-python-ng

COPY receiver/ /app/receiver/

EXPOSE 8000

CMD ["uvicorn", "receiver.app:app", "--host", "0.0.0.0", "--port", "8000"]
