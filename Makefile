# From https://raw.githubusercontent.com/vincentbernat/hellogopher/1cf9ebeeaea41d2e159e196f7d176d6b12ff0eb2/Makefile
# Licensed under Creative Commons Zero v1.0 Universal (CC0)

MODULE   = $(shell env GO111MODULE=on $(GO) list -m)
DATE    ?= $(shell date +%FT%T%z)
VERSION ?= $(shell git describe --tags --always --dirty --match=v* 2> /dev/null || \
			cat .version 2> /dev/null || echo v0)
PKGS     = $(or $(PKG),$(shell env GO111MODULE=on $(GO) list ./...))
BUILD    = build
BIN      = $(BUILD)/bin
TEST     = $(BUILD)/test

GO      = go
TIMEOUT = 15
V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell [ "$$(tput colors 2> /dev/null || echo 0)" -ge 8 ] && printf "\033[34;1m▶\033[0m" || printf "▶")

export GO111MODULE=on

.PHONY: all
all: check-license mocks deep-copy fmt lint test-coverage | $(BIN)

# Tools

$(BUILD):
	@mkdir -p $@

$(BIN):
	@mkdir -p $@

$(TEST):
	@mkdir -p $@

$(BIN)/%: | $(BIN) ; $(info $(M) installing $(PACKAGE)...)
	$Q if [ ! -z "INSTALL_ROOT" ]; then cd $(INSTALL_ROOT); fi; env GOBIN=$(abspath $(BIN)) $(GO) install $(PACKAGE)

GOIMPORTS = $(BIN)/goimports
$(BIN)/goimports: PACKAGE=golang.org/x/tools/cmd/goimports@latest

REVIVE = $(BIN)/revive
$(BIN)/revive: PACKAGE=github.com/mgechev/revive@latest

GOCOV = $(BIN)/gocov
$(BIN)/gocov: PACKAGE=github.com/axw/gocov/gocov@latest

GOCOVXML = $(BIN)/gocov-xml
$(BIN)/gocov-xml: PACKAGE=github.com/AlekSi/gocov-xml@latest

GOTESTSUM = $(BIN)/gotestsum
$(BIN)/gotestsum: PACKAGE=gotest.tools/gotestsum@latest

ADDLICENSE = $(BIN)/addlicense
$(BIN)/addlicense: PACKAGE=github.com/google/addlicense@latest

# FIXME this might break in the future since Mockery does not officially support installing with go install
MOCKERY = $(BIN)/mockery
$(BIN)/mockery: PACKAGE=github.com/vektra/mockery/v2@v2.12.3

# Note: deepcopy-gen contains replace directives and cannot be installed in module-aware mode,
# instead it is checked out and installed from the checked-out directory.
DEEPCOPY = $(BIN)/deepcopy-gen
$(BIN)/deepcopy-gen: PACKAGE=k8s.io/gengo/examples/deepcopy-gen
$(BIN)/deepcopy-gen: INSTALL_ROOT=./$(GENGO_DIR)/examples/deepcopy-gen

GENGO_DIR = $(BUILD)/src/gengo
$(GENGO_DIR): ; $(info $(M) cloning Gengo...)
	@mkdir -p $@
	$Q git clone -q https://github.com/kubernetes/gengo.git $@
	$Q cd $@; git checkout -q 4627b89bbf1b # v0.0.0-20220307231824-4627b89bbf1b

# Tests

test-bench: NAME=benchmarks ## Run benchmarks
test-bench: ARGS=-run=__absolutelynothing__ -bench=.

test-short: NAME=short tests ## Run only short tests
test-short: ARGS=-short

test-verbose: NAME=tests in verbose mode ## Run tests in verbose mode
test-verbose: GOTESTSUM_ARGS=--format=standard-verbose

test-race: NAME=tests with race detection ## Run tests with race detection
test-race: ARGS=-race

COVERAGE_MODE = atomic
test-coverage: NAME=tests with coverage ## Run tests with coverage
test-coverage: ARGS=-coverpkg=$(shell echo $(PKGS) | tr ' ' ',') -covermode=$(COVERAGE_MODE) -coverprofile=$(TEST)/profile.out $(PKGS)
test-coverage: | $(GOCOV) $(GOCOVXML)
	$Q $(GO) tool cover -html=$(TEST)/profile.out -o $(TEST)/coverage.html
	$Q $(GOCOV) convert $(TEST)/profile.out | $(GOCOVXML) > $(TEST)/coverage.xml
	@echo "Code coverage: "; \
		echo "scale=1;$$(sed -En 's/^<coverage line-rate="([0-9.]+)".*/\1/p' $(TEST)/coverage.xml) * 100 / 1" | bc -q

TEST_TARGETS := test-bench test-short test-verbose test-race test-coverage
$(TEST_TARGETS): test
.PHONY: $(TEST_TARGETS) check test tests

check test tests: | $(GOTESTSUM) $(TEST) ; $(info $(M) running $(if $(NAME),$(NAME),tests)...) @ ## Run tests
	$Q $(GOTESTSUM) $(GOTESTSUM_ARGS) --junitfile $(TEST)/tests.xml -- -timeout $(TIMEOUT)s $(ARGS) $(PKGS)

# Formatting and linting

.PHONY: lint
lint: | $(REVIVE) ; $(info $(M) running golint...) @ ## Run golint
	$Q $(REVIVE) -formatter friendly -set_exit_status ./...

.PHONY: fmt
fmt: | $(GOIMPORTS) ; $(info $(M) running gofmt...) @ ## Run gofmt on all source files
	$Q $(GOIMPORTS) -local $(MODULE) -w $(shell $(GO) list -f '{{$$d := .Dir}}{{range $$f := .GoFiles}}{{printf "%s/%s\n" $$d $$f}}{{end}}{{range $$f := .CgoFiles}}{{printf "%s/%s\n" $$d $$f}}{{end}}{{range $$f := .TestGoFiles}}{{printf "%s/%s\n" $$d $$f}}{{end}}' $(PKGS))

# Misc

.PHONY: clean
clean: ; $(info $(M) cleaning...) @ ## Cleanup everything (deletes build directory)
	@rm -rf $(BUILD)

.PHONY: help
help: ## Displays this help message
	@grep -hE '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-17s\033[0m %s\n", $$1, $$2}'

.PHONY: version
version: ## Displays the current version on this git branch
	@echo $(VERSION)

# See https://stackoverflow.com/questions/52242077/go-modules-finding-out-right-pseudo-version-vx-y-z-timestamp-commit-of-re
PSEUDO_VERSION ?= $(shell TZ=UTC git --no-pager show --quiet --abbrev=12 --date='format-local:%Y%m%d%H%M%S' --format="%cd-%h" 2> /dev/null || echo v0)
.PHONY: pseudo-version
pseudo-version: ## Displays the current pseudo-version on this git branch
	@echo $(PSEUDO_VERSION)

# License

check-license: | $(ADDLICENSE) ; $(info $(M) checking license headers...) @ ## Checks license headers in all source files
	$Q $(ADDLICENSE) -check -c "DataStax" -ignore '.github/**'  -ignore '.idea/**' -ignore '$(BUILD)/**' . 2> /dev/null

update-license: | $(ADDLICENSE) ; $(info $(M) updating license headers...) @ ## Updates license headers in all source files
	$Q $(ADDLICENSE) -c "DataStax" -ignore '.github/**'  -ignore '.idea/**' -ignore '$(BUILD)/**' . 2> /dev/null

# Mocks

.PHONY: mock mocks
mock mocks: | $(MOCKERY) ; $(info $(M) generating mocks...) @ ## Generates mocks
	$Q $(MOCKERY) --quiet --dir=./datacodec --name=Codec             --output=./datacodec --outpkg=datacodec --filename=mock_codec_test.go               --structname=mockCodec
	$Q $(MOCKERY) --quiet --dir=./datacodec --name=injector          --output=./datacodec --outpkg=datacodec --filename=mock_injector_test.go            --structname=mockInjector
	$Q $(MOCKERY) --quiet --dir=./datacodec --name=extractor         --output=./datacodec --outpkg=datacodec --filename=mock_extractor_test.go           --structname=mockExtractor
	$Q $(MOCKERY) --quiet --dir=./datacodec --name=keyValueExtractor --output=./datacodec --outpkg=datacodec --filename=mock_key_value_extractor_test.go --structname=mockKeyValueExtractor
	$Q $(MOCKERY) --quiet --dir=./datacodec --name=keyValueInjector  --output=./datacodec --outpkg=datacodec --filename=mock_key_value_injector_test.go  --structname=mockKeyValueInjector

# Deep Copy

deep-copy: | $(GENGO_DIR) $(DEEPCOPY) ; $(info $(M) generating deepcopy methods...) @ ## Generates DeepCopy methods
	$Q $(DEEPCOPY) -h deepcopy-header.txt -i github.com/datastax/go-cassandra-native-protocol/primitive --trim-path-prefix github.com/datastax/go-cassandra-native-protocol -o .
	$Q $(DEEPCOPY) -h deepcopy-header.txt -i github.com/datastax/go-cassandra-native-protocol/message   --trim-path-prefix github.com/datastax/go-cassandra-native-protocol -o .
	$Q $(DEEPCOPY) -h deepcopy-header.txt -i github.com/datastax/go-cassandra-native-protocol/datatype  --trim-path-prefix github.com/datastax/go-cassandra-native-protocol -o .
	$Q $(DEEPCOPY) -h deepcopy-header.txt -i github.com/datastax/go-cassandra-native-protocol/frame     --trim-path-prefix github.com/datastax/go-cassandra-native-protocol -o .
	$Q $(DEEPCOPY) -h deepcopy-header.txt -i github.com/datastax/go-cassandra-native-protocol/segment   --trim-path-prefix github.com/datastax/go-cassandra-native-protocol -o .
