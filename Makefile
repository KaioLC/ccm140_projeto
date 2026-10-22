.PHONY: help

help:
	@echo "Comandos:"
	@echo "  make docker-up		- faz o build e sobe os containers"
	@echo "  make docker-down	- para e derruba os containers"

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down
