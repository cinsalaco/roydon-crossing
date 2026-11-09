# Plain Docker build: avoids Railpack having to fetch its toolchain from GitHub
# at build time (which failed with 504s on 9 Nov 2026).
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["python", "app.py"]
