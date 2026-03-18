FROM golang:1.23-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libzmq3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .
RUN go mod download

RUN CGO_ENABLED=1 GOOS=linux go build -o /servidor ./servidores/go/
RUN CGO_ENABLED=1 GOOS=linux go build -o /cliente ./clientes/go/

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libzmq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /servidor /servidor
COPY --from=builder /cliente /cliente

CMD ["/servidor"]
