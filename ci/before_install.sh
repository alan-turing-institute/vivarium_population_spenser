#!/bin/bash

uname -a
free -m
df -h
ulimit -a
mkdir builds
pushd builds

# Build into our own virtualenv
pip install -U virtualenv

virtualenv --python=python venv

source venv/bin/activate
python -V

pip install --upgrade pip setuptools

if [ $TRAVIS_PULL_REQUEST = "false" ]
then
    branch=$TRAVIS_BRANCH
else
    branch=$TRAVIS_PULL_REQUEST_BRANCH
fi

echo branch $branch

upstream_branch=git ls-remote --heads git@github.com:ihmeuw/vivarium.git $branch

echo upstream branch found $upstream_branch

if [ $upstream_branch -eq 1]
then
    "git clone git@github.com:ihmeuw/vivarium.git"
    pushd vivarium
    git checkout $branch
    pip install .
    popd
fi

popd
