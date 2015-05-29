#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from compdb.contrib import get_project

def main():
    project = get_project()
    docs = project.find()
    for doc in docs:
        print(doc)

if __name__ == '__main__':
    logging.basicConfig(level = logging.WARNING)
    main()