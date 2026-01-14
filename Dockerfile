FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy
WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt
RUN playwright install chromium
COPY scraper.py .
COPY morocco_cities.json .
ENV NODE_OPTIONS="--max-old-space-size=4096"
CMD ["python", "scraper.py"]