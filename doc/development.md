# Technical Framework

## Use relational db and non-relational db in conjunction

Use standard relational db for django framework.
Use non-relational db, such as MongoDB, to store everything else.

## Tool assessment

### cmake

Used as primary build system.

Minimum version required: 2.8

### Python

Full support for: Python 3.3
Minimal support for: Python 2.7

### Django

Version 1.7

### numpy

Minimum version required: 1.8

### mongodb-engine

Using MongoDB as primary database for django requires a mongodb backend for django.
`djanog-mongod-db` provides such a backend, but is in an early development stage and does not provide support for python 3.
All in all it seems very risky to use this engine at this point, as it has not yet reached a productive level.
