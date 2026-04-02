FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    MALLOC_ARENA_MAX=2 \
    MALLOC_TRIM_THRESHOLD_=131072

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts
COPY .env.example ./.env.example

RUN mkdir -p /app/data /app/logs \
    && useradd --create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)" || exit 1

# Keep the default worker count low because each worker loads its own analytics state.
CMD ["gunicorn", "app.main:app", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--workers", "1", "--timeout", "90", "--graceful-timeout", "30", "--keep-alive", "30", "--access-logfile", "-", "--error-logfile", "-"]
