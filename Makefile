SHELL := bash

PROJECT_NAME := $(shell sed -n 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml | head -1)
PROJECT_VERSION := $(shell sed -n 's/^[[:space:]]*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml | head -1)
ifeq ($(PROJECT_NAME),)
    $(error Error: Cargo.toml package name not found or invalid)
endif

TOP_DIR := $(CURDIR)
CARGO := cargo
EXAMPLE ?= main
PREFIX ?= $(HOME)/.local

HAS_REL := $(shell command -v git-rel 2>/dev/null)

$(info ------------------------------------------)
$(info Project: $(PROJECT_NAME) v$(PROJECT_VERSION))
$(info ------------------------------------------)

.PHONY: build b compile c run r benchmark benchmark-compare benchmark-memory benchmark-merge benchmark-typed benchmark-write-verification fixtures operator operator-size package test t check check-all test-all clippy rustdoc fmt fmt-check loc-check clean verify release help h

build:
	@$(CARGO) build --lib

b: build

compile:
	@$(CARGO) clean
	@$(MAKE) build

c: compile

run:
	@$(CARGO) run --example $(EXAMPLE)

r: run

benchmark:
	@$(CARGO) run --release --example benchmark

benchmark-compare:
	@$(CARGO) run --release --example benchmark_jammdb

benchmark-memory:
	@$(CARGO) run --release --example benchmark_memory

benchmark-merge:
	@$(CARGO) run --release --example benchmark_merge

benchmark-typed:
	@$(CARGO) run --release --features typed --example benchmark_typed_reads

benchmark-write-verification:
	@$(CARGO) run --release --example benchmark_write_verification

fixtures:
	@$(CARGO) run --example fixture_gen

operator:
	@$(CARGO) build --release --features operator --bin tagdata

operator-size:
	@$(CARGO) build --profile size --features operator --bin tagdata

package:
	@$(CARGO) package --locked

test:
	@$(CARGO) test --all-targets

t: test

check:
	@$(CARGO) check --all-targets

check-all:
	@$(CARGO) check --all-targets --all-features

fmt:
	@$(CARGO) fmt --all

fmt-check:
	@$(CARGO) fmt --all -- --check

clippy:
	@$(CARGO) clippy --all-targets --all-features -- -D warnings

rustdoc:
	@RUSTDOCFLAGS="-Dwarnings" $(CARGO) doc --all-features --no-deps

loc-check:
	@find src tests examples -type f -name '*.rs' -print0 | \
		xargs -0 wc -l | \
		awk '$$2 != "total" && $$1 > 800 { print "error: " $$2 " has " $$1 " lines (maximum 800)"; failed = 1 } END { exit failed }'

test-all:
	@$(CARGO) test --all-targets --all-features

clean:
	@$(CARGO) clean

verify: fmt-check loc-check check test check-all test-all clippy rustdoc

release:
	@if [ -z "$(HAS_REL)" ]; then \
		echo "git-rel is not installed. Please install it first."; \
		exit 1; \
	fi
	@if [ -z "$(TYPE)" ]; then \
		echo "Release type not specified. Use 'make release TYPE=[patch|minor|major|M.m.p]'"; \
		exit 1; \
	fi
	@git rel $(TYPE)

help:
	@echo
	@echo "Usage: make [target]"
	@echo
	@echo "Available targets:"
	@echo "  build        Build the library"
	@echo "  compile      Clean and rebuild"
	@echo "  run          Run a development example"
	@echo "  benchmark    Run the release-mode smoke benchmark"
	@echo "  benchmark-compare  Compare Tagdata with jammdb 0.11.0"
	@echo "  benchmark-memory   Measure reopen and write-transaction heap allocations"
	@echo "  benchmark-merge    Merge two 500k-entry random KV databases"
	@echo "  benchmark-typed    Compare direct typed scans with the previous lookup path"
	@echo "  benchmark-write-verification  Compare write-safety policies"
	@echo "  fixtures     Regenerate the frozen current-format fixture"
	@echo "  operator     Build the release-mode operator CLI"
	@echo "  operator-size  Build the size-optimized operator CLI"
	@echo "  package      Verify and package the locked crate"
	@echo "  test         Run all tests"
	@echo "  check        Run cargo check on all targets"
	@echo "  check-all    Run cargo check on all targets/all features"
	@echo "  test-all     Run cargo test on all targets/all features"
	@echo "  clippy       Run clippy with warnings denied"
	@echo "  rustdoc      Build docs with warnings denied"
	@echo "  fmt          Format the workspace"
	@echo "  loc-check    Reject Rust source or test files over 800 lines"
	@echo "  fmt-check    Check formatting"
	@echo "  clean        Remove Cargo build artifacts"
	@echo "  verify       Run the full local gate"
	@echo "  release      Release a new version"
	@echo

h: help
