FROM python:3.12-slim
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/folder_audit.py api/main.py api/cron.py ./

EXPOSE 8000

RUN echo "* * * * * cd /app && /usr/local/bin/python cron.py >> /var/log/cron.log 2>&1" > /etc/cron.d/app-cron && \
    chmod 0644 /etc/cron.d/app-cron && \
    crontab /etc/cron.d/app-cron && \
    touch /var/log/cron.log
CMD ["sh", "-c", "python cron.py init && uvicorn main:app --host 0.0.0.0 --port 8000"]