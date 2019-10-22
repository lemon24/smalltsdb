.PHONY:

all: test

install-dev:
	pip install -q -e '.[cli,dev]'

test: clean-pyc install-dev
	pytest -v --runslow

coverage: clean-pyc install-dev
	pytest --cov -v --runslow
	coverage html

cov: coverage

test-all: install-dev
	tox

docs: clean-pyc install-dev
	 $(MAKE) -C docs html

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -rf {} +
