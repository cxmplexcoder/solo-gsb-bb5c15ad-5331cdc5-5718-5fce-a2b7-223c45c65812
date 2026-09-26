FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CALIB_DATA_DIR=/data \
    CALIB_BACKUP_DIR=/backups

WORKDIR /app

# 依赖在构建期安装；容器启动时由 FastAPI lifespan 自动完成数据库初始化
# （建表 + 取得与恢复工具互斥的文件锁）。
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY samples ./samples

RUN mkdir -p /data /backups
VOLUME ["/data", "/backups"]

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/api/health', timeout=3).status == 200 else 1)"

# 默认启动 Web/API 服务。
# 备份/恢复通过覆盖命令调用同一镜像内的工具，例如：
#   docker compose exec calibration python -m app.backup backup
#   docker compose run --rm calibration python -m app.backup restore --yes /backups/xxx.db
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
