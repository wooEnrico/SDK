#!/usr/bin/env bash
TAG=$1

if [ -z $TAG ]; then
  echo "Please provide a tag"
  exit 1
fi

set -e

git checkout $TAG

mvn clean
mvn javadoc:aggregate

cd target/site
mv apidocs/ docs

git init -b apidocs
git add .
git commit -m "Update docs for $TAG"
git push -f git@github.com:wooEnrico/SDK.git apidocs

cd ../../
git switch master



