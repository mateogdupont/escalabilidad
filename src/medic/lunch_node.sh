#!/bin/bash

# Verifica si se ha pasado un argumento
if [ $# -eq 0 ]; then
    exit 1
fi

# Accede al argumento del string pasado desde Python
container=$1

# Imprime el string recibido desde Python
docker compose up -d "$container"
