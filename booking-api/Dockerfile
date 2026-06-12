FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
# dockerignore: ad8227d fix *.json exclusion
# cachebuster: ad8227d
RUN apt-get update \
    && apt-get install -y --no-install-recommends socat \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY app /app/app
# cachebuster: ad8227d rebuild with dockerignore fix
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh
# noop: trigger rebuild after tenant api fix

CMD ["/app/start.sh"]
