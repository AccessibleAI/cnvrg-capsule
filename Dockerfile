FROM golang:1.16.5 as builder
WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download
COPY main.go ./
COPY pkg/ pkg/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -o capsule main.go

FROM ubuntu:20.04
WORKDIR /opt/app-root
RUN ln -fs /usr/share/zoneinfo/Israel /etc/localtime \
    && apt -y update \
    && apt -y install postgresql curl jq
COPY --from=builder /workspace/capsule .
