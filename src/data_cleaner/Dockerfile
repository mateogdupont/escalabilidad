FROM python:3.9.7-slim

# Copy the application directory and the utils directory
COPY ./data_cleaner ./data_cleaner
COPY ./utils ./utils

# Install the required Python packages for utils
RUN pip install -r ./utils/requirements.txt

ENV PYTHONPATH / 

CMD bash -c "sleep 2 && python data_cleaner/data_cleaner.py"
