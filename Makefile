
build-mac:
	go build -v -o bin/capsule-darwin-x86_64 main.go

build-linux:
	docker run --rm -v ${PWD}:/usr/src/cnvrgctl -w /usr/src/cnvrgctl golang:1.16 /bin/bash -c "GO111MODULE=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/capsule-linux-x86_64 main.go"

install-mac: build-mac
	mv ./bin/capsule-darwin-x86_64 /usr/local/bin/cnvrgctl
	cnvrgctl completion bash > /usr/local/etc/bash_completion.d/cnvrgctl

docker-build:
	docker build . -t docker.io/cnvrg/cnvrg-capsule:v1.0

docker-push:
	docker push docker.io/cnvrg/cnvrg-capsule:v1.0

.PHONY: deploy
deploy:
	kubectl apply -f deploy/dep.yaml

.PHONY: test
test:
	go test ./pkg/backup/... -v

