.PHONY: help setup pull status clean run stop

REPOS_FILE = core.repos

help:
	@echo "Comandos:"
	@echo "  make repos-import	- clona todos os repositórios do $(REPOS_FILE)"
	@echo "  make repos-pull	- atualiza os repositorios"
	@echo "  make docker-up		- faz o build e sobe os containers"
	@echo "  make docker-down	- para e derruba os containers"

repos-import:
	@mkdir -p $(WORKSPACE)
	vcs import $(WORKSPACE) < $(REPOS_FILE)
repos-pull:
	vcs pull $(WORKSPACE)

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down
