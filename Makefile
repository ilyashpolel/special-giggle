SHELL := /usr/bin/env bash

.PHONY: test lint build coverage run docker-up docker-down tools

test:
	$(MAKE) -C aws_stuff test

lint:
	$(MAKE) -C aws_stuff lint

build:
	$(MAKE) -C aws_stuff build

coverage:
	$(MAKE) -C aws_stuff coverage

run:
	$(MAKE) -C aws_stuff run CMD="$(CMD)"

docker-up:
	$(MAKE) -C aws_stuff docker-up

docker-down:
	$(MAKE) -C aws_stuff docker-down

tools:
	$(MAKE) -C aws_stuff tools 