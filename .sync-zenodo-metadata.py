#!/usr/bin/env python
"""Synchronize authors and contributor metadata.

This script synchronizes the metadata provided in the CITATION.cff and
the contributors.yaml file with the metadata stored in the .zenodo.json
file.

All authors should be listed in the CITATION.cff file, while contributors
should be listed in the contributors.yaml file.
Both files use the [citation file-format][1] for [person objects][2].

[1] https://citation-file-format.github.io/
[2] https://citation-file-format.github.io/1.0.3/specifications/#/person-objects
"""
from dataclasses import dataclass
from typing import Optional

import click
import json
from ruamel.yaml import load, Loader


@dataclass
class Contributor:
    last_names: str
    first_names: str
    affiliation: str
    orcid: Optional[str] = None

    @classmethod
    def from_citation_author(cls, **citation):
        return cls(
            last_names=citation.pop('family-names'),
            first_names=citation.pop('given-names'),
            **citation)

    def as_zenodo_creator(self):
        ret = dict(
            name='{} {}'.format(self.first_names, self.last_names),
            affiliation=self.affiliation)
        if self.orcid:
            ret['orcid'] = self.orcid.lstrip('https://orcid.org/')
        return ret


@click.command()
@click.pass_context
@click.option('--check', default=False, is_flag=True,
              help="Return with non-zero exit code if metadata needs to be updated.")
@click.option('-i', '--in-place', type=bool, is_flag=True,
              help="Modify metadata in place.")
def sync(ctx, in_place=False, check=True):

    with open('CITATION.cff', 'rb') as file:
        citation = load(file.read(), Loader=Loader)
        authors = [
            Contributor.from_citation_author(**author)
            for author in citation['authors']]

    with open('contributors.yaml', 'rb') as file:
        contributors = load(file.read(), Loader=Loader)['contributors']
        contributors = [Contributor.from_citation_author(
            **contributor) for contributor in contributors]

    with open('.zenodo.json', 'rb') as file:
        zenodo = json.loads(file.read())
        zenodo_updated = zenodo.copy()
        zenodo_updated['creators'] = [a.as_zenodo_creator() for a in authors]
        zenodo_updated['contributors'] = [c.as_zenodo_creator()
                                          for c in contributors if c not in authors]
        for key in ('version', 'keywords'):
            zenodo_updated[key] = citation[key]

    modified = json.dumps(zenodo, sort_keys=True) != json.dumps(zenodo_updated, sort_keys=True)
    if modified:
        if in_place:
            with open('.zenodo.json', 'wb') as file:
                file.write(json.dumps(zenodo_updated, indent=4, sort_keys=True).encode('utf-8'))
        else:
            click.echo(json.dumps(zenodo_updated, indent=4, sort_keys=True))
        if check:
            ctx.exit(1)
    else:
        click.echo("No changes.", err=True)


if __name__ == '__main__':
    sync()
