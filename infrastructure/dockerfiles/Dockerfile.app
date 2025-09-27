FROM python:3.11-slim
WORKDIR /app
RUN pip install fastapi uvicorn psycopg2-binary redis celery
CMD ["sleep", "infinity"]  # Temporary for testing