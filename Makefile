BUILD_VERSION       := $(shell date "+%F %T")
COMMIT_ID           := $(shell git rev-parse --short HEAD)
BUILD_VERSION       := 'main.BuildVersion=${COMMIT_ID}(${BUILD_VERSION})'
all:

	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o fcbench-arm -ldflags "-s -w -X ${BUILD_VERSION}" cmd/bench_fctsdb_v2/*.go
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o fcbench-linux -ldflags "-s -w -X ${BUILD_VERSION}" cmd/bench_fctsdb_v2/*.go
	go build -o fcbench -ldflags "-s -w -X ${BUILD_VERSION}" cmd/bench_fctsdb_v2/*.go

.PHONY : all
