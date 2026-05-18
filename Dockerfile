FROM golang:1.25-alpine AS build

ARG GOPROXY=https://proxy.golang.org,direct
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG ALL_PROXY
ARG NO_PROXY

ENV GOPROXY=${GOPROXY}
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV ALL_PROXY=${ALL_PROXY}
ENV NO_PROXY=${NO_PROXY}

WORKDIR /src

COPY analytics-service ./analytics-service
COPY analytics-core ./analytics-core

WORKDIR /src/analytics-service

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/simpletrack-anaysitics-service ./cmd/simpletrack-anaysitics-service

FROM alpine:3.22

RUN adduser -D -u 10001 simpletrack

WORKDIR /app

COPY --from=build /out/simpletrack-anaysitics-service /usr/local/bin/simpletrack-anaysitics-service
COPY --from=build /src/analytics-service/api ./api
COPY --from=build /src/analytics-service/public ./public

EXPOSE 8080

USER simpletrack

ENTRYPOINT ["simpletrack-anaysitics-service"]
