setup:
	@echo "Creating virtual environment with Python $$(cat .python-version)..."
	python$$(cat .python-version) -m venv .venv
	@if [ -n "$$BASH_VERSION" ]; then \
		source .venv/bin/activate; \
	else \
		. .venv/bin/activate; \
	fi && \
	pip install poetry==1.8.2 && \
	poetry install

clean:
	- deactivate 2>/dev/null || true
	rm -rf .venv
