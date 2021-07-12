
build-mac:
	go build -v -o bin/capsule-darwin-x86_64 main.go

install-mac: build-mac
	mv ./bin/capsule-darwin-x86_64 /usr/local/bin/cnvrgctl
	cnvrgctl completion bash > /usr/local/etc/bash_completion.d/cnvrgctl

docker-build:
	docker build . -t docker.io/cnvrg/cnvrg-capsule:v0.2

docker-push:
	docker push docker.io/cnvrg/cnvrg-capsule:v0.2

.PHONY: deploy
deploy:
	kubectl apply -f deploy/dep.yaml