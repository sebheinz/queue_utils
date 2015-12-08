#!/bin/sh

if [ $# -ne 1 ]; then
    >&2 echo "usage: $0 <version>"
    exit 1
fi

set -xe

python --version
git --version

version=$1

sed -i "s/__version__ = .*/__version__ = '${version}'/" */__init__.py

# Do not tag/push on Go CD
if [ -z "$GO_PIPELINE_LABEL" ]; then
    python setup.py clean
    python setup.py test
    python setup.py flake8

    git add */__init__.py

    git commit -m "Bumped version to $version"
    git push
fi

python setup.py sdist upload

if [ -z "$GO_PIPELINE_LABEL" ]; then
    git tag ${version}
    git push --tags
fi
