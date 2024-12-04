FROM postgres:13

RUN apt-get update \
    && apt-get install -y postgis postgresql-13-postgis-3 \
    && rm -rf /var/lib/apt/lists/*

COPY init/ /docker-entrypoint-initdb.d/