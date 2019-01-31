# How to contribute to the project

## Feedback

The report of issues and the proposal of new features is very welcome.
Please use the [GitHub issue page](https://github.com/glotzerlab/signac/issues/) for this.

## Contributing code

Code contributions to the signac open-source project are welcomed via pull requests on GitHub.
Prior any work you should contact the signac developers to ensure that the planned development meshes well with the directions and standards of the project.
All contributors must agree to the Contributor Agreement ([ContributorAgreement.md](ContributorAgreement.md)) before their pull request can be merged.

General guidelines:

  * Use the [git flow][git flow] model of development:
    - New features should be developed in a feature branch based on `develop`.
    - Bug fixes should be based on `master` if they affect a current release of the package.
  * Write code that is compatible with supported versions of Python (listed in `setup.py`).
  * Avoid introducing dependencies -- especially those that might be harder to install in high-performance computing environments.
  * Use [flake8](http://flake8.pycqa.org/en/latest/) and [autopep8](https://pypi.org/project/autopep8/) to find and fix any code style issues.
  * Create [unit tests](https://en.wikipedia.org/wiki/Unit_testing) and [integration tests](https://en.wikipedia.org/wiki/Integration_testing) that cover the common cases and the corner cases of the code.
  * Preserve backwards-compatibility whenever possible, and make clear if something must change.
  * Document any portions of the code that might be less clear to others, especially to new developers.
  * Write API documentation in this package, and put usage information, guides, and concept overviews in the [framework documentation](https://docs.signac.io/) ([source][signac-docs-src]).

[signac-docs-src]: https://github.com/glotzerlab/signac-docs/
[gitflow]: https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow

Please see the [Support](https://docs.signac.io/projects/signac/en/latest/support.html) section as part of the documentation for detailed development guidelines.
