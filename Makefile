BINDIR      := $(CURDIR)/bin
DIST_DIRS   := find * -type d -exec
TARGETS     := darwin/amd64 linux/amd64 linux/386 linux/arm linux/arm64 linux/ppc64le linux/s390x
TARGET_OBJS ?= darwin-amd64.tar.gz darwin-amd64.tar.gz.sha256 darwin-amd64.tar.gz.sha256sum linux-amd64.tar.gz linux-amd64.tar.gz.sha256 linux-amd64.tar.gz.sha256sum linux-386.tar.gz linux-386.tar.gz.sha256 linux-386.tar.gz.sha256sum linux-arm.tar.gz linux-arm.tar.gz.sha256 linux-arm.tar.gz.sha256sum linux-arm64.tar.gz linux-arm64.tar.gz.sha256 linux-arm64.tar.gz.sha256sum linux-ppc64le.tar.gz linux-ppc64le.tar.gz.sha256 linux-ppc64le.tar.gz.sha256sum linux-s390x.tar.gz linux-s390x.tar.gz.sha256 linux-s390x.tar.gz.sha256sum
BINNAME     ?= pg2parquet

GOPATH        = $(shell go env GOPATH)
DEP           = $(GOPATH)/bin/dep
GOX           = $(GOPATH)/bin/gox
GOIMPORTS     = $(GOPATH)/bin/goimports
ARCH          = $(shell uname -p)

PKG        := ./...
TAGS       :=
TESTS      := .
TESTFLAGS  :=
LDFLAGS    := -w -s
GOFLAGS    :=
SRC        := $(shell find . -type f -name '*.go' -print)

SHELL      = /usr/bin/env sh

GIT_COMMIT = $(shell git rev-parse HEAD)
GIT_SHA    = $(shell git rev-parse --short HEAD)_dist
ifdef VERSION
	BINARY_VERSION = $(VERSION)
endif
BINARY_VERSION ?= "unreleased"

ifneq ($(BINARY_VERSION),)
	LDFLAGS += -X github.com/electric-saw/pg2parquet/internal/version.version=${BINARY_VERSION}
endif

VERSION_METADATA = unreleased
ifneq ($(GIT_TAG),)
	VERSION_METADATA =
endif

LDFLAGS += -X github.com/electric-saw/pg2parquet/internal/version.metadata=${VERSION_METADATA}
LDFLAGS += -X github.com/electric-saw/pg2parquet/internal/version.gitCommit=${GIT_COMMIT}
LDFLAGS += -X github.com/electric-saw/pg2parquet/internal/version.gitTreeState=${GIT_DIRTY}
.PHONY: all
run:
	@GO111MODULE=on go run . $(word 2, $(MAKECMDGOALS) )

.PHONY: all
all: build

.PHONY: build
build: $(BINDIR)/$(BINNAME)

$(BINDIR)/$(BINNAME): $(SRC)
	GO111MODULE=on go build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $(BINDIR)/$(BINNAME) .

.PHONY: test
test: build
ifeq ($(ARCH),s390x)
test: TESTFLAGS += -v
else
test: TESTFLAGS += -race -v
endif
test: test-style
test: test-unit

.PHONY: test-unit
test-unit:
	@echo
	@echo "==> Running unit tests <=="
	GO111MODULE=on go test $(GOFLAGS) -run $(TESTS) $(PKG) $(TESTFLAGS)

.PHONY: test-style
test-style:
	GO111MODULE=on golangci-lint run

.PHONY: format
format: $(GOIMPORTS)
	GO111MODULE=on go list -f '{{.Dir}}' ./... | xargs $(GOIMPORTS) -w -local .

$(GOX):
	(cd /; GO111MODULE=on go get -u github.com/mitchellh/gox)

$(GOIMPORTS):
	(cd /; GO111MODULE=on go get -u golang.org/x/tools/cmd/goimports)

.PHONY: build-cross
build-cross: LDFLAGS += -extldflags "-static"
build-cross: $(GOX)
	GO111MODULE=on CGO_ENABLED=0 $(GOX) -parallel=4 -output="_dist/{{.OS}}-{{.Arch}}/$(BINNAME)" -osarch='$(TARGETS)' $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' .

.PHONY: dist
dist: build-cross
	( \
		cd _dist && \
		$(DIST_DIRS) cp ../LICENSE {} \; && \
		$(DIST_DIRS) cp ../README.md {} \; && \
		$(DIST_DIRS) tar -zcf pg2parquet-${VERSION}-{}.tar.gz {} \; && \
		$(DIST_DIRS) zip -r pg2parquet-${VERSION}-{}.zip {} \; \
	)
.PHONY: checksum
checksum:
	for f in _dist/*.{gz,zip} ; do \
		shasum -a 256 "$${f}" | sed 's/_dist\///' > "$${f}.sha256sum" ; \
		shasum -a 256 "$${f}" | awk '{print $$1}' > "$${f}.sha256" ; \
	done

.PHONY: clean
clean:
	@rm -rf $(BINDIR) ./_dist

.PHONY: release-notes
release-notes:
		@if [ ! -d "./_dist" ]; then \
			echo "please run 'make fetch-release' first" && \
			exit 1; \
		fi
		@if [ -z "${PREVIOUS_RELEASE}" ]; then \
			echo "please set PREVIOUS_RELEASE environment variable" \
			&& exit 1; \
		fi

		@./scripts/release-notes.sh ${PREVIOUS_RELEASE} ${VERSION}

.PHONY: info
info:
	 @echo "Version:           ${VERSION}"
	 @echo "Git Tag:           ${GIT_TAG}"
	 @echo "Git Commit:        ${GIT_COMMIT}"
	 @echo "Git Tree State:    ${GIT_DIRTY}"

.PHONY: default_hash
default_hash:
	@uuidgen --name pg2parquet --namespace @url github.com/electric-saw/pg2parquet --sha1

dist-complete: dist checksum
