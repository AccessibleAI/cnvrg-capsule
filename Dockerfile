# Build the manager binary
FROM golang:1.16.5 as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY main.go ./
COPY pkg/ pkg/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -o capsule main.go

FROM ubuntu:20.04
WORKDIR /opt/app-root
RUN ln -fs /usr/share/zoneinfo/Israel /etc/localtime \
    && apt -y update \
    && apt -y install postgresql
COPY --from=builder /workspace/capsule .
