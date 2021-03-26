#!/bin/bash

mkdir -p static/javadoc
find ../javadoc -name "*.tar.gz" -exec tar xzvf {} -Cstatic/javadoc \;
