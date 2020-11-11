# How to Contribute to the Project

## Providing Feedback

Issue reports and feature proposals are very welcome.
Please use the [GitHub issue tracker](https://github.com/glotzerlab/signac/issues/) for this.

## Writing Documentation

A general introduction in the form of tutorials, guides, and recipes is published as part of the framework documentation at [https://docs.signac.io](https://docs.signac.io).
The API of each package as part of the framework is documented in the form of doc-strings, which are published on `https://docs.signac.io/projects/$package`, where `$package` is currently one of `core`, `flow`, or `dashboard`.

Anyone is invited to add to or edit any part of the documentation.
To fix a spelling mistake or make minor edits, click on the **Edit on GitHub** button in the top-right corner.
For more substantial edits, consider cloning the [signac-docs repository](https://github.com/glotzerlab/signac-docs) to a local computer.

## Triaging Issues

Any contributor is invited to triage new issues by applying any of the existing [labels](https://github.com/glotzerlab/signac/labels).

## Contributing Code

Code contributions to the signac open-source project are welcomed via pull requests on GitHub.
Prior any work you should contact the signac developers to ensure that the planned development meshes well with the directions and standards of the project.
All contributors must agree to the Contributor Agreement ([ContributorAgreement.md](ContributorAgreement.md)) before their pull request can be merged.

### Guideline for Code Contributions

* Use the [OneFlow](https://www.endoflineblog.com/oneflow-a-git-branching-model-and-workflow) model of development:
  - Both new features and bug fixes should be developed in branches based on `master`.
  - Hotfixes (critical bugs that need to be released *fast*) should be developed in a branch based on the latest tagged release.
* Write code that is compatible with all supported versions of Python (listed in [setup.py](https://github.com/glotzerlab/signac/blob/master/setup.py)).
* Avoid introducing dependencies -- especially those that might be harder to install in high-performance computing environments.
* Create [unit tests](https://en.wikipedia.org/wiki/Unit_testing) and [integration tests](https://en.wikipedia.org/wiki/Integration_testing) that cover the common cases and the corner cases of the code.
* Preserve backwards-compatibility whenever possible, and make clear if something must change.
* Document any portions of the code that might be less clear to others, especially to new developers.
* Write API documentation in this package, and put usage information, guides, and concept overviews in the [framework documentation](https://docs.signac.io/) ([source](https://github.com/glotzerlab/signac-docs/)).
* Use inclusive language in all documentation and code. The [Google developer documentation style guide](https://developers.google.com/style/inclusive-documentation) is a helpful reference.

Please see the [Support](https://docs.signac.io/projects/signac-core/en/latest/support.html) section as part of the documentation for detailed development guidelines.

### Code Style

The [pre-commit tool](https://pre-commit.com/) is used to enforce code style guidelines. Use `pip install pre-commit` to install the tool and `pre-commit install` to configure pre-commit hooks.

## Reviewing Pull Requests

Pull requests represent the standard way of contributing code to the code base.
Each pull request is assigned to one of the project committers, who is responsible for triaging it, finding at least two reviewers (one of whom can be themselves), and eventually merging or closing the pull request.
Pull requests should generally be approved by two reviewers prior to merge.

### Guidelines for Pull Request Reviewers

The following general guidelines should be considered during the pull request review process:

* API breaking changes should be avoided whenever possible and require approval by a project maintainer.
* Significant performance degradations must be avoided unless the regression is necessary to fix a bug.
* Non-trivial bug fixes should be accompanied by a unit test that catches the related issue to avoid future regression.
* The code should be easy to follow and sufficiently documented to be understandable even to developers who are not highly familiar with the code.
* Code duplication should be avoided and existing classes and functions are effectively reused.
* The pull request is on-topic and does not introduce multiple independent changes (such as unrelated style fixes).
* A potential increase in code complexity introduced with a pull request is well justified by the benefits of the added feature.
* The API of a new feature is well-documented in the doc-strings and usage is documented as part of the [framework documentation](https://github.com/glotzerlab/signac-docs).
