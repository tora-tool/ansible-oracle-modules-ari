#!/usr/bin/env sh

coverage run "$(which nosetests)" -v
coverage report -m
