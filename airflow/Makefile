
.DEFAULT_GOAL := help
.PHONY: help sync dev restart stop kill deploy pytest

help:  ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

sync:  ## Rsync scripts/omdb_fetch.py + dbt/ + pbi/ z repo root do airflow/include/
	bash bin/sync-include.sh

dev: sync  ## Sync + astro dev start (4 kontenery na localhost:6563)
	astro dev start

restart: sync  ## Sync + astro dev restart (rebuild image z cache + restart kontenerow)
	astro dev restart

stop:  ## astro dev stop (zachowuje kontenery, mozna wznowic z `make dev`)
	astro dev stop

kill:  ## astro dev kill (usuwa kontenery - clean state, kolejny `make dev` startuje od zera)
	astro dev kill

deploy: sync  ## Sync + astro deploy (push do Astronomer Cloud)
	astro deploy

pytest:  ## astro dev pytest (DAG integrity tests wewnatrz scheduler kontenera)
	astro dev pytest
