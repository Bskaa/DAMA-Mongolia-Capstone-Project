FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir "meltano==4.4.1"

COPY entrypoint.py /usr/local/bin/meltano-entrypoint.py

WORKDIR /project
ENTRYPOINT ["python", "/usr/local/bin/meltano-entrypoint.py"]
