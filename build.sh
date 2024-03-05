#!/bin/bash



hadoop com.sun.tools.javac.Main -d dest src/drivers/Alibaba.java src/mappers/*.java src/reducers/*.java src/writables/*.java
cd dest
jar cf Alibaba.jar drivers/Alibaba.class mappers/*.class reducers/*.class writables/*.class

find . -type f -name '*.class' -delete