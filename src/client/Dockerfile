FROM python:3.9.7-slim

# Copy the application directory and the utils directory
COPY ./client ./client
COPY ./utils ./utils

# Copy the env file
COPY .env ./.env

# Install the required Python packages for utils
RUN pip install -r ./utils/requirements.txt

# Install unzip
RUN apt-get update && apt-get install -y unzip

ENV PYTHONPATH / 

CMD bash -c "unzip -n /data.zip && python client/main.py .. 3"
