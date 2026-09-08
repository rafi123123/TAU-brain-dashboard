FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY .env api/folder_audit.py api/main.py api/cron.py api/cron.sh ./
RUN chmod +x ./cron.sh


COPY api/app-crontab /etc/cron.d/
RUN chmod 0644 /etc/cron.d/app-crontab
RUN touch /var/log/cron.log

RUN apt-get update -o Acquire::Languages=none -o Acquire::IndexTargets::deb::Contents-deb=false \
    && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8000
CMD ["sh", "-c", "cron && python cron.py init && uvicorn main:app --host 0.0.0.0 --port 8000"]