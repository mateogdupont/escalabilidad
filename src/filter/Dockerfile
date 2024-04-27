FROM python:3.9.7-slim

# Copy the application directory and the utils directory
COPY ./filter ./filter
COPY ./utils ./utils

# Install the required Python packages for utils
RUN pip install -r ./utils/requirements.txt

ENV PYTHONPATH / 

CMD bash -c "python filter/filter.py"
