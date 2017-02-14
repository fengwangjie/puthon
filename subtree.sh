#!/bin/bash

if [ $# -eq 0 ]
    then
        echo "Usage: ./subtree <command>"
        exit 1
fi

case $1 in
    "pull") git subtree pull --prefix common git@bitbucket.org:wisdomgarden/data-integration.git master --squash;;
    "push") git subtree push --prefix common git@bitbucket.org:wisdomgarden/data-integration.git master;;
    *) echo "sorry unknown command $1";;
esac