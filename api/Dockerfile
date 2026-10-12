FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY api/.env api/alembic.ini api/*.py ./
COPY api/alembic/ ./alembic/
COPY api/db/ ./db/

EXPOSE 8000
ENV PYTHONUNBUFFERED=1
# apply the alembic migrations before serving, so the schema always matches the code
CMD ["sh", "-c", "alembic upgrade head && exec uvicorn main:app --host 0.0.0.0 --port 8000"]
