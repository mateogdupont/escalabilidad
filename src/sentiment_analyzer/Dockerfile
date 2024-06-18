FROM python:3.9-slim

WORKDIR /app

# Copy the application directory and the utils directory
COPY ./sentiment_analyzer ./sentiment_analyzer
COPY ./utils ./utils

# Install the required Python packages for utils
RUN pip install --upgrade pip
RUN pip install -r ./utils/requirements.txt

# Install the required Python packages for the sentiment_analyzer
RUN pip install -r ./sentiment_analyzer/requirements.txt

# Set the Python path to include /app
ENV PYTHONPATH=/app

# CMD ["python", "sentiment_analyzer/main.py"]
ENTRYPOINT ["bash", "-c", "sleep 10 && python sentiment_analyzer/analyzer.py"]
