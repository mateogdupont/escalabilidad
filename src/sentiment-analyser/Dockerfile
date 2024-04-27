FROM python:3.9-slim

WORKDIR /app

# Copy the application directory and the utils directory
COPY ./sentiment-analyser ./sentiment-analyser
COPY ./utils ./utils

# Install the required Python packages for utils
RUN pip install -r ./utils/requirements.txt

# Install the required Python packages for the sentiment-analyser
RUN pip install -r ./sentiment-analyser/requirements.txt

# Set the Python path to include /app
ENV PYTHONPATH=/app

CMD ["python", "sentiment-analyser/main.py"]