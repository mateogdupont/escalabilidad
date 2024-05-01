FROM python:3.9.7-slim

# Copy the application directory and the utils directory
COPY ./counter ./counter
COPY ./utils ./utils

# Copy the env file
COPY .env ./.env

# Install the required Python packages for utils
RUN pip install -r ./utils/requirements.txt

# Install the required Python packages for counter
RUN pip install -r ./counter/requirements.txt

ENV PYTHONPATH / 

CMD bash -c "python counter/counter.py"
