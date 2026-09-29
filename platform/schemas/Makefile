# Schema contract — generate, verify, or check conformance.
#
# The .proto files are the source of truth; `generated/` is derived and
# committed (see README.md). Regenerate after any .proto edit and commit the
# result — CI runs `check` and fails on drift.
#
# Runs inside a container so no host protobuf toolchain is needed: the image
# carries grpcio-tools, which ships protoc. `buf` is used instead when it is
# installed.

SCHEMAS_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
IMAGE       ?= python:3.12-slim
TOOLS       := grpcio-tools==1.84.0

.PHONY: help generate check conformance clean

help:
	@echo "make generate    regenerate generated/ from proto/ (then commit it)"
	@echo "make check       fail if generated/ has drifted from proto/ (CI gate)"
	@echo "make conformance normalise real corpus lines against the contract"

generate:
	docker run --rm -v "$(SCHEMAS_DIR):/schemas" -w /schemas $(IMAGE) \
		sh -c 'pip install -q "$(TOOLS)" && python tools/generate_schemas.py'

check:
	docker run --rm -v "$(SCHEMAS_DIR):/schemas" -w /schemas $(IMAGE) \
		sh -c 'pip install -q "$(TOOLS)" && python tools/generate_schemas.py --check'

# Needs a corpus: the real node dump, or a CI fixture. Override with
#   make conformance CORPUS=/path/to/data_stream_with_block_info
CORPUS ?= /nvme0n1-disk/data/hl-node-data/data_stream_with_block_info
conformance:
	docker run --rm -v "$(SCHEMAS_DIR):/schemas:ro" -v "$(CORPUS):/corpus:ro" \
		-w /schemas $(IMAGE) \
		sh -c 'pip install -q pyarrow==25.0.1 orjson==3.12.0 && \
		       python tools/check_corpus_conformance.py /corpus'

clean:
	rm -rf generated/arrow/hypercore_arrow/__pycache__ generated/__pycache__
