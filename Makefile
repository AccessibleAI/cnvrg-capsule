build-mac:
	go build -ldflags="-X 'main.Build=$$(git rev-parse --short HEAD)' -X 'main.Version=$$(cat /tmp/newCapsuleVersion)'" -v -o bin/capsule-darwin-x86_64 main.go

build-linux: docker-build
	docker run -v ${PWD}:/tmp --rm -it docker.io/cnvrg/cnvrg-capsule:$(shell cat /tmp/newCapsuleVersion)  bash -c "cp /opt/app-root/capsule /tmp/bin/capsule-linux-x86_64"

install-mac: build-mac
	mv ./bin/capsule-darwin-x86_64 /usr/local/bin/capsule
	capsule completion bash > /usr/local/etc/bash_completion.d/capsule

docker-build:
	docker build --build-arg BUILD_SHA=$(shell git rev-parse --short HEAD) --build-arg BUILD_VERSION=$(shell cat /tmp/newCapsuleVersion) -t docker.io/cnvrg/cnvrg-capsule:$(shell cat /tmp/newCapsuleVersion) .

docker-push:
	docker push docker.io/cnvrg/cnvrg-capsule:$(shell cat /tmp/newCapsuleVersion)

.PHONY: deploy
deploy:
	TAG=$(shell cat /tmp/newCapsuleVersion) envsubst < deploy/dep.yaml | kubectl apply -f -

.PHONY: test
test:
	source hack/aws-test-bucket-creds.sh && go test ./pkg/backup/... -v

unfocus:
	ginkgo unfocus


current-version:
	{ \
	set -e ;\
	currentVersion=$$(git fetch --tags && git tag -l --sort -version:refname | head -n 1) ;\
	echo $$currentVersion > /tmp/newCapsuleVersion ;\
    }


override-release: current-version docker-build docker-push 
	git tag -d $$(cat /tmp/newCapsuleVersion)
	git push origin -d $$(cat /tmp/newCapsuleVersion)
	git tag $$(cat /tmp/newCapsuleVersion)
	git push origin $$(cat /tmp/newCapsuleVersion)

patch-release: patch-version docker-build docker-push
	git tag $$(cat /tmp/newCapsuleVersion);
	git push origin $$(cat /tmp/newCapsuleVersion)

minor-release: minor-version docker-build docker-push
	git tag $$(cat /tmp/newCapsuleVersion);
	git push origin $$(cat /tmp/newCapsuleVersion)

major-release: major-version docker-build docker-push
	git tag $$(cat /tmp/newCapsuleVersion);
	git push origin $$(cat /tmp/newCapsuleVersion)


patch-version:
	{ \
	set -e ;\
	currentVersion=$$(git fetch --tags && git tag -l --sort -version:refname | head -n 1) ;\
	patchVersion=$$(echo $$currentVersion | tr . " " | awk '{print $$3}') ;\
	patchVersion=$$(( $$patchVersion + 1 )) ;\
	newVersion=$$(echo $$currentVersion | tr . " " | awk -v pv=$$patchVersion '{print $$1"."$$2"."pv}') ;\
	echo $$newVersion > /tmp/newCapsuleVersion ;\
    }

minor-version:
	{ \
	set -e ;\
	currentVersion=$$(git fetch --tags && git tag -l --sort -version:refname | head -n 1) ;\
	minorVersion=$$(echo $$currentVersion | tr . " " | awk '{print $$2}') ;\
	minorVersion=$$(( $$minorVersion + 1 )) ;\
	newVersion=$$(echo $$currentVersion | tr . " " | awk -v pv=$$minorVersion '{print $$1"."pv"."0}') ;\
	echo $$newVersion > /tmp/newCapsuleVersion ;\
    }

major-version:
	{ \
	set -e ;\
	currentVersion=$$(git fetch --tags && git tag -l --sort -version:refname | head -n 1) ;\
	majorVersion=$$(echo $$currentVersion | tr . " " | awk '{print $$1}') ;\
	majorVersion=$$(( $$majorVersion + 1 )) ;\
	newVersion=$$(echo $$currentVersion | tr . " " | awk -v pv=$$majorVersion '{print pv".0.0"}') ;\
	echo $$newVersion > /tmp/newCapsuleVersion ;\
    }