FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MERCHMIND_DATA_DIR=/app/data

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir '.[warehouse]'

COPY data ./data
COPY sql ./sql
EXPOSE 8000 8501
CMD ["uvicorn", "merchmind.api:app", "--host", "0.0.0.0", "--port", "8000"]
