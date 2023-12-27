#!/usr/bin/env bash
function rc() {

  git tag v$RC -m "$MESSAGE"
  mvn versions:set -DnewVersion=$RC -DgenerateBackupPoms=false

  export MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED"
  mvn test
  mvn clean
  mvn deploy
  git push git@github.com:wooEnrico/SDK.git --tags
  git checkout .
}

echo "RC Version: "
read RC

echo "RC message: "
read MESSAGE

echo "(rc version is $RC, rc message is $MESSAGE) Are You Sure? [Y/n]"
read input

case $input in
	[yY])
	  rc
		;;
	[nN])
		echo "exit"
		exit 1
		;;
	*)
		echo "Invalid input & exit"
		exit 1
esac


