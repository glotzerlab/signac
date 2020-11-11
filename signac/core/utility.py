# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Utility classes for signac version."""

import re
import subprocess

from deprecation import deprecated

from ..version import __version__


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="All database related functions have been deprecated.",
)
def get_subject_from_certificate(fn_certificate):  # noqa: D103, E261
    try:
        cert_txt = subprocess.check_output(
            [
                "openssl",
                "x509",
                "-in",
                fn_certificate,
                "-inform",
                "PEM",
                "-subject",
                "-nameopt",
                "RFC2253",
            ]
        ).decode()
    except subprocess.CalledProcessError:
        msg = "Unable to retrieve subject from certificate '{}'."
        raise RuntimeError(msg.format(fn_certificate))
    else:
        lines = cert_txt.split("\n")
        assert lines[0].startswith("subject=")
        return lines[0][len("subject=") :].strip()


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
