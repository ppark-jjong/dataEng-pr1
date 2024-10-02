# Use Python image as the base
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY ./config/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source files
COPY ./src /app/src
COPY ./proto /app/proto

# Copy OAuth credentials
COPY ./oauth /app/oauth

# Expose the default Kafka port (9092) if needed for testing Kafka
EXPOSE 9092

# Start the main Python script
CMD ["python", "/app/src/main.py"]
