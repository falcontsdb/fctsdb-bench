BUILD_VERSION       := $(shell date "+%F %T")
COMMIT_ID           := $(shell git rev-parse --short HEAD)
BRANCH              := $(shell git rev-parse --abbrev-ref HEAD)
BUILD_VERSION       := 'main.BuildVersion=${BRANCH}(${COMMIT_ID}) ${BUILD_VERSION}'

all:

	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o fcbench-arm64 -ldflags "-s -w -X ${BUILD_VERSION}" cmd/*.go
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o fcbench-amd64 -ldflags "-s -w -X ${BUILD_VERSION}" cmd/*.go
	go build -o fcbench -ldflags "-s -w -X ${BUILD_VERSION}" cmd/*.go

.PHONY : all
