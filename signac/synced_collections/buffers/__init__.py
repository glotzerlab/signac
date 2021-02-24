# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Defines the buffering protocol for synced collections.

In addition to defining the buffering protocol for synced collections in
:class:`~.BufferedCollection`, this subpackage also defines a number of
supported buffering implementations. No buffers are imported by default. Users
should import desired buffers as needed.
"""
from typing import List as _List

__all__: _List[str] = []
