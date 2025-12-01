# Etapa de construcción
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copiar definiciones de módulos
COPY go.mod go.sum ./
RUN go mod download

# Copiar código fuente
COPY . .

# Compilar binarios
RUN go build -o /bin/master cmd/master/main.go
RUN go build -o /bin/worker cmd/worker/main.go
RUN go build -o /bin/client cmd/client/main.go

# Etapa final (Imagen ligera)
FROM alpine:latest

WORKDIR /app

# Crear directorios necesarios
RUN mkdir -p /tmp/mini-spark
RUN mkdir -p /app/data

# Copiar binarios desde el builder
COPY --from=builder /bin/master /usr/local/bin/master
COPY --from=builder /bin/worker /usr/local/bin/worker
COPY --from=builder /bin/client /usr/local/bin/client

# Por defecto, ejecutar el master (se sobreescribe en docker-compose)
CMD ["master"]