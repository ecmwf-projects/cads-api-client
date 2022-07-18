PROJECT := cads_api_client
CONDA := conda
CONDAFLAGS :=
COV_REPORT := html

default: qa test type-check

qa:
	pre-commit run --all-files

test:
	python -m pytest -vv --cov=. --cov-report=$(COV_REPORT)

type-check:
	python -m mypy --strict .

conda-env-update:
	$(CONDA) env update $(CONDAFLAGS) -f environment.yml

docker-build:
	docker build -t $(PROJECT) .

docker-run:
	docker run --rm -ti -v $(PWD):/srv $(PROJECT)

template-update:
	pre-commit run --all-files cruft -c .pre-commit-config-weekly.yaml

build-docs:
	cd docs && rm -fr _api && make clean && make html
