FROM python:3.9.7-slim

# Copy the application directory and the utils directory
COPY ./filter ./filter
COPY ./utils ./utils

# Copy the env file
COPY .env ./.env

# Install the required Python packages for utils
RUN pip install --upgrade pip
RUN pip install -r ./utils/requirements.txt

ENV PYTHONPATH / 

# Sleep before starts -> gives time to the MOM to start
ENTRYPOINT ["bash", "-c", "sleep 10 && python filter/filter.py"]