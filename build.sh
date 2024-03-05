#!/bin/bash

hadoop com.sun.tools.javac.Main -d . $(find src -name "*.java")
jar cf Alibaba.jar *.class
rm *.class