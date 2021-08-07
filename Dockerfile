FROM golang:1.16.5 as builder
ARG BUILD_SHA
ARG BUILD_VERSION
WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download
COPY main.go ./
COPY pkg/ pkg/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on \
    go build \
    -ldflags="-X 'main.Build=${BUILD_SHA}' -X 'main.Version=${BUILD_VERSION}'" \
    -o capsule main.go

FROM ubuntu:20.04
WORKDIR /opt/app-root
RUN ln -fs /usr/share/zoneinfo/Israel /etc/localtime \
    && apt -y update \
    && apt -y install postgresql curl jq
COPY --from=builder /workspace/capsule /opt/app-root/capsule
