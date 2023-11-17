#!/usr/bin/env bash
function release() {

  mvn versions:set -DnewVersion=$RELEASE -DgenerateBackupPoms=false
  git add .
  git commit --message "Release for $RELEASE"

  git tag v$RELEASE -m "v$RELEASE"

  export MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED"
  mvn clean test deploy

  mvn versions:set -DnewVersion=$SNAPSHOT -DgenerateBackupPoms=false
  git add .
  git commit --message "Snapshot for $SNAPSHOT"

#  git push git@github.com:wooEnrico/SDK.git master
#  git push git@github.com:wooEnrico/SDK.git --tags
}

echo "Release Version: "
read RELEASE

echo "Snapshot Version: "
read SNAPSHOT

echo "(release version is $RELEASE, snapshot version is $SNAPSHOT) Are You Sure? [Y/n]"
read input

case $input in
	[yY])
	  release
		;;
	[nN])
		echo "exit"
		exit 1
		;;
	*)
		echo "Invalid input & exit"
		exit 1
esac


