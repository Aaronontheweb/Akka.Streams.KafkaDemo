FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app

ENV KAFKA_HOST "kafka"
ENV KAFKA_PORT "29092"
ENV KAFKA_SASL_USERNAME ""
ENV KAFKA_SASL_PASSWORD ""

COPY ./bin/Release/net5.0/publish/ /app

ENTRYPOINT ["dotnet", "Akka.Streams.KafkaDemo.Producer.dll"]
