# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Utility classes for signac version."""

import re


class Version(dict):
    """Utility class to manage revision control numbers."""

    def __init__(self, major=0, minor=0, change=0, postrelease="", prerelease="final"):
        if prerelease > "final":
            raise ValueError("illegal pre-release tag", prerelease)
        super().__init__(
            major=major,
            minor=minor,
            change=change,
            postrelease=postrelease,
            prerelease=prerelease,
        )

    def to_tuple(self):
        """Return version details as tuple."""
        return (
            self["major"],
            self["minor"],
            self["change"],
            self["prerelease"],
            self["postrelease"],
        )

    def __lt__(self, other):
        return self.to_tuple() < other.to_tuple()

    def __eq__(self, other):
        return self.to_tuple() == other.to_tuple()

    def __str__(self):
        return "{major}.{minor}{postrelease}.{change}{prerelease}".format(**self)

    def __repr__(self):
        return "Version({})".format(",".join((f"{k}={v}" for k, v in self.items())))


def parse_version(version_str):
    """Parse a version number into a version object."""
    p = re.compile(
        r"(?P<major>[0-9]*)\.(?P<minor>[0-9]*)((?P<postrelease>-?\w*)"
        r"\.(?P<change>[0-9])(?P<prerelease>\w*))?"
    )
    r = p.match(version_str)
    v = r.groupdict()
    version = Version(
        **{
            "major": int(v.get("major") or 0),
            "minor": int(v.get("minor") or 0),
            "change": int(v.get("change") or 0),
            "postrelease": str(v.get("postrelease") or ""),
            "prerelease": str(v.get("prerelease") or "final"),
        }
    )
    return version
