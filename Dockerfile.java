FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    openjdk-17-jdk-headless \
    protobuf-compiler \
    libprotobuf-dev \
    libprotobuf-java \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN mkdir -p lib shared/java/protos out

RUN curl -fsSL -o lib/jeromq-0.6.0.jar \
      https://repo1.maven.org/maven2/org/zeromq/jeromq/0.6.0/jeromq-0.6.0.jar \
  && curl -fsSL -o lib/jnacl-1.0.0.jar \
      https://repo1.maven.org/maven2/eu/neilalexander/jnacl/1.0.0/jnacl-1.0.0.jar

RUN curl -fsSL -o lib/gson-2.10.1.jar \
      https://repo1.maven.org/maven2/com/google/code/gson/gson/2.10.1/gson-2.10.1.jar

RUN cp -v /usr/share/java/*protobuf*jar lib/ || true

RUN protoc -I./contratos -I/usr/include --java_out=./shared/java/protos ./contratos/contrato.proto

RUN find . -name '*.java' ! -name 'Dockerfile.java' > sources.txt \
  && javac -encoding UTF-8 -cp "lib/*" -d out @sources.txt

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/out /app/out
COPY --from=builder /app/lib /app/lib

ENTRYPOINT ["java","-cp","/app/out:/app/lib/*"]
CMD ["ServidorJava"]

