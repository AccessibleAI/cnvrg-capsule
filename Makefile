
build-mac:
	go build -v -o bin/capsule-darwin-x86_64 main.go

.PHONY: install-mac
install-mac: build-mac
	mv ./bin/capsule-darwin-x86_64 /usr/local/bin/cnvrgctl
	cnvrgctl completion bash > /usr/local/etc/bash_completion.d/cnvrgctl