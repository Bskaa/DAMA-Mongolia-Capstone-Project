FROM apache/airflow:3.3.2-python3.11

USER root
RUN mkdir -p /opt/project-venv /opt/meltano \
    && chown -R airflow:root /opt/project-venv /opt/meltano

USER airflow
COPY --chown=airflow:root requirements.txt /tmp/project-requirements.txt
RUN python -m venv /opt/project-venv \
    && /opt/project-venv/bin/pip install --no-cache-dir \
        -r /tmp/project-requirements.txt "meltano==4.4.1"

COPY --chown=airflow:root meltano/meltano.yml /opt/meltano/meltano.yml
ENV MELTANO_PROJECT_ROOT=/opt/meltano
RUN cd /opt/meltano && /opt/project-venv/bin/meltano install
