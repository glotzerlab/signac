# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

from signac.core.utility import Version, parse_version

# ordered by release time
TEST_VERSIONS = [
    ('0.1.2dev1', Version(minor=1, change=2, prerelease='dev1')),
    ('0.1.2', Version(minor=1, change=2)),
    ('0.1-a1.2', Version(major=0, minor=1, postrelease='-a1', change=2)),
    ('0.1-a1.3dev1', Version(minor=1, postrelease='-a1', change=3, prerelease='dev1')),
    ('0.1.3', Version(minor=1, change=3)),
    ('1.0', Version(major=1)),
    ('1.2', Version(major=1, minor=2)),
    ('10.23', Version(major=10, minor=23)),
]


class TestVersionNumbering:
    def test_init(self):
        Version()

    def test_parsing(self):
        for vs, v in TEST_VERSIONS:
            assert v == parse_version(vs)

    def test_equal(self):
        for vs, v in TEST_VERSIONS:
            p = parse_version(vs)
            assert p == p

    def test_comparison(self):
        for i in range(0, len(TEST_VERSIONS) - 1):
            v0 = TEST_VERSIONS[i][0]
            v1 = TEST_VERSIONS[i + 1][0]
            assert parse_version(v0) < parse_version(v1)

    def test_illegal_prelease_tag(self):
        with pytest.raises(ValueError):
            Version(prerelease='final1')
