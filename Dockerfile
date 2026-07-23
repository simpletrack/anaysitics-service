FROM golang:1.25-alpine AS build

ARG GOPROXY=https://proxy.golang.org,direct
ARG GIT_PROTOCOL=https
ARG GIT_CLONE_TOKEN
ARG GIT_CLONE_REF=main
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG ALL_PROXY
ARG NO_PROXY

ENV GOPROXY=${GOPROXY}
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV ALL_PROXY=${ALL_PROXY}
ENV NO_PROXY=${NO_PROXY}

RUN apk add --no-cache git openssh-client

WORKDIR /src

# SSH clone — requires --ssh default in the build command
# (e.g. docker build --ssh default ...)
RUN --mount=type=ssh \
  if [ "${GIT_PROTOCOL}" = "ssh" ]; then \
    mkdir -p -m 0700 /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    git clone --branch "${GIT_CLONE_REF}" --depth 1 git@github.com:simpletrack/analytics-service.git /src/analytics-service; \
  fi

# HTTPS clone — with or without a personal access token
RUN if [ "${GIT_PROTOCOL}" != "ssh" ] && [ -n "${GIT_CLONE_TOKEN}" ]; then \
    git clone --branch "${GIT_CLONE_REF}" --depth 1 https://${GIT_CLONE_TOKEN}@github.com/simpletrack/analytics-service.git /src/analytics-service; \
  elif [ "${GIT_PROTOCOL}" != "ssh" ]; then \
    git clone --branch "${GIT_CLONE_REF}" --depth 1 https://github.com/simpletrack/analytics-service.git /src/analytics-service; \
  fi

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
