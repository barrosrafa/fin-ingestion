# syntax=docker/dockerfile:1.4
FROM python:3.12-slim-bookworm AS builder

WORKDIR /app

# Instala dependências de compilação essenciais
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

# Instala gerenciador uv
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

# Instala dependências do projeto com cache
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Estágio de runtime otimizado e seguro
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Copia código da aplicação e dbt
COPY fin_ingestion/ /app/fin_ingestion/
COPY dbt/ /app/dbt/
COPY migrations/ /app/migrations/
COPY scripts/ /app/scripts/
COPY pyproject.toml /app/

# Usuário não-root
RUN useradd -m -u 1000 finuser && chown -R finuser:finuser /app
USER finuser

# Ponto de entrada padrão
CMD ["python", "-m", "fin_ingestion.workers.runner"]
