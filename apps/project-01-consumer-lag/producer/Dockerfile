FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run as non-root
RUN useradd --create-home --uid 1000 appuser
USER appuser

CMD ["python", "-u", "producer.py"]
