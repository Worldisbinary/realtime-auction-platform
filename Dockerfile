FROM python:3.12-slim AS builder
WORKDIR /src
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
COPY pyproject.toml README.md ./
COPY app ./app
RUN pip install --no-cache-dir ".[postgres]"

FROM python:3.12-slim
ENV PATH="/venv/bin:$PATH" PYTHONUNBUFFERED=1
COPY --from=builder /venv /venv
RUN useradd --create-home --uid 10001 app
USER app
WORKDIR /home/app
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "app.main:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8000"]
