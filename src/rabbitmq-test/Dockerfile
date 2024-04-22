FROM python:3.9-slim

WORKDIR /app

# Install pika
RUN pip install pika

# Copy the Python script
COPY send_receive.py .

# Copy the .env file
COPY .env .

# Copy the requirements.txt file
COPY requirements.txt .

# Install the required Python packages
RUN pip install -r requirements.txt

CMD ["python", "send_receive.py"]
