setup:
	@echo "Creating virtual environment with Python $$(cat .python-version)..."; \
	if ! command -v pyenv >/dev/null 2>&1; then \
		echo "❌ pyenv is not installed or not on PATH."; \
		exit 1; \
	fi; \
	# Install Java if not installed \
	if ! command -v java >/dev/null 2>&1; then \
		echo "Java not found. Installing OpenJDK 17..."; \
		sudo apt update && sudo apt install -y openjdk-17-jdk; \
	else \
		echo "Java found."; \
	fi; \
	export JAVA_HOME=$$(readlink -f $$(which java) | sed "s:bin/java::"); \
	echo "JAVA_HOME is set to $$JAVA_HOME"; \
	pyenv install -s $$(cat .python-version); \
	PYENV_PYTHON=$$(pyenv which python); \
	$$PYENV_PYTHON -m venv .venv && \
	echo "✅ Virtual environment created with $$PYENV_PYTHON" && \
	. .venv/bin/activate && \
	pip install poetry==1.8.2 && \
	poetry install

clean:
	- deactivate 2>/dev/null || true
	rm -rf .venv
