#!/bin/bash

# Verifica si se ha pasado un argumento
if [ $# -eq 0 ]; then
    exit 1
fi

# Accede al argumento del string pasado desde Python
container=$1

# Imprime el string recibido desde Python (ver qué flags son necesarias -> ver el force recreate)
docker compose up -d --no-deps "$container" 
