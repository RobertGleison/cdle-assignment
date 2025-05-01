setup:
	@echo "Creating virtual environment with Python $$(cat .python-version)..."; \
	if ! command -v pyenv >/dev/null 2>&1; then \
		echo "❌ pyenv is not installed or not on PATH."; \
		exit 1; \
	fi; \
	pyenv install -s $$(cat .python-version); \
	PYENV_PYTHON=$$(pyenv which python); \
	$$PYENV_PYTHON -m venv .venv && \
	echo "✅ Virtual environment created with $$PYENV_PYTHON" && \
	. .venv/bin/activate && \
	pip install poetry==1.8.2 && \
	poetry install

install_rapids:
	- pip install \
    --extra-index-url=https://pypi.nvidia.com \
    "cudf-cu11==25.4.*" "dask-cudf-cu11==25.4.*"

clean:
	- deactivate 2>/dev/null || true
	rm -rf .venv
