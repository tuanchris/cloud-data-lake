all: build export rmi

build:
	docker build -t docker-airflow-cloud .

export:
	docker save -o docker-airflow-cloud.docker docker-airflow-cloud

up:
	docker-compose -f docker-compose.yml up

run:
	docker-compose -f docker-compose.yml up -d

down:
	docker-compose -f docker-compose.yml down

rmi:
	docker images|grep none|awk '{ print $3}'|xargs docker rmi -f

rebuild:
	docker build -t docker-airflow-cloud . --no-cache

restart:
	docker-compose down && docker-compose up -d
logs:
	docker-compose logs -tf
