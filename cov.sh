#!/bin/bash
pytest --cov signac --cov-config=setup.cfg --cov-report=xml tests/ -v $@
coverage html
