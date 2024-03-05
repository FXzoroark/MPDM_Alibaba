#!/bin/bash

# BASEDIR=$(dirname $0)
# ZIPPATH=${BASEDIR}/corpus.zip

# echo $ZIPPATH

# wget -O ${ZIPPATH} "https://filesender.renater.fr/download.php?token=eb9a1555-3ad0-413a-a46f-c5a7c81596ad&files_ids=34789481"  
# unzip ${ZIPPATH}
# rm ${ZIPPATH}

hadoop namenode -format
hadoop datanode -format
start-all.sh
hdfs dfsadmin -safemode leave

hadoop fs -mkdir -p Alibaba/
hadoop fs -copyFromLocal Corpus/selectionCourt.csv Alibaba/
hadoop fs -copyFromLocal Corpus/selectionComplete.csv Alibaba/


#mkdir dest/