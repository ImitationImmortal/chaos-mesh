FROM golang:1.20 as builder

WORKDIR /builder/

COPY . .

ENV CGO_ENABLED=0

RUN version_ldflags=$(hack/version.sh) && \
    go build -ldflags "$version_ldflags" -o images/chaos-mesh/bin/chaos-controller-manager cmd/chaos-controller-manager/main.go

FROM alpine:3.17

ARG HTTPS_PROXY
ARG HTTP_PROXY

ENV http_proxy $HTTP_PROXY
ENV https_proxy $HTTPS_PROXY

RUN apk add tzdata --no-cache

COPY --from=builder /builder/images/chaos-mesh/bin/chaos-controller-manager /usr/local/bin/chaos-controller-manager
