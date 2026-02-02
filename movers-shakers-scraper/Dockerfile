# ✅ Uses Playwright’s image with browsers + deps preinstalled
FROM mcr.microsoft.com/playwright/python:v1.43.0-jammy

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install Python deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your app
COPY . .

# Run your script
CMD ["python", "test.py"]
