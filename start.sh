#!/bin/bash

hadoop jar dest/Alibaba.jar drivers.Alibaba -D mapred.reduce.tasks=2 Alibaba/selectionCourt.csv Alibaba/res 