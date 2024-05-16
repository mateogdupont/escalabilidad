FROM python:3.9-slim

WORKDIR /app

# Copy the application directory and the utils directory
COPY ./sentiment-analyzer ./sentiment-analyzer
COPY ./utils ./utils

# Install the required Python packages for utils
RUN pip install -r ./utils/requirements.txt

# Install the required Python packages for the sentiment-analyzer
RUN pip install -r ./sentiment-analyzer/requirements.txt

# Set the Python path to include /app
ENV PYTHONPATH=/app

# Sleep before starts -> gives time to the MOM to start
ENTRYPOINT ["bash", "-c", "sleep 10 && python sentiment-analyzer/analyzer.py"]
