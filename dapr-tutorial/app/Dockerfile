# One local image for all three services; each Deployment picks its uvicorn module.
FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea
WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r requirements.lock \
    && pip check
COPY . .
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN useradd --uid 1000 --no-create-home app
USER app
EXPOSE 8080
CMD ["uvicorn", "frontend.main:app", "--host", "0.0.0.0", "--port", "8080"]
