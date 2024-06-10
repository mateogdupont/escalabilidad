FROM python:3.9.7-slim

#Install docker
RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli


# Copy the application directory
COPY . .

# Install the required Python packages for utils
RUN pip install --upgrade pip
RUN pip install -r ./utils/requirements.txt

ENV PYTHONPATH /

ENTRYPOINT ["bash", "-c", "python medic/medic.py"]