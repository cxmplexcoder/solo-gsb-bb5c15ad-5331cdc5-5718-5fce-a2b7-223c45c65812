FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CALIB_DATA_DIR=/data \
    CALIB_BACKUP_DIR=/backups

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY samples ./samples

RUN mkdir -p /data /backups
VOLUME ["/data"]

EXPOSE 8000

# 启动即完成初始化：lifespan 内获取数据目录互斥锁并执行 init_db 建表。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
