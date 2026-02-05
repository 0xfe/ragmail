PACKAGE_NAME ?= ragmail
PACKAGE_VERSION ?= dev
PACKAGE_DIR ?= dist
PACKAGE_FILE ?= $(PACKAGE_DIR)/$(PACKAGE_NAME)-$(PACKAGE_VERSION).tar.gz

EXCLUDES = \
	--exclude .git \
	--exclude .venv \
	--exclude .ragmail-cache \
	--exclude workspaces \
	--exclude private \
	--exclude embeddings \
	--exclude *.lancedb \
	--exclude *.embed.db \
	--exclude *.mbox \
	--exclude *.clean.jsonl \
	--exclude *.spam.jsonl \
	--exclude *.gz \
	--exclude *.tgz \
	--exclude *.summary \
	--exclude .pytest_cache \
	--exclude .mypy_cache \
	--exclude .ruff_cache \
	--exclude __pycache__ \
	--exclude dist \
	--exclude build

.PHONY: package package-dev

package:
	@mkdir -p $(PACKAGE_DIR)
	@if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
		git archive --format=tar.gz --output $(PACKAGE_FILE) HEAD; \
	else \
		tar $(EXCLUDES) -czf $(PACKAGE_FILE) .; \
	fi
	@echo "Wrote $(PACKAGE_FILE)"

package-dev:
	@mkdir -p $(PACKAGE_DIR)
	@tar $(EXCLUDES) -czf $(PACKAGE_FILE) .
	@echo "Wrote $(PACKAGE_FILE)"
