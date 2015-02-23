#!/bin/bash

echo "export HADOOP_USER_CLASSPATH_FIRST=true" > /home/hadoop/conf/hadoop-user-env.sh

echo "export HADOOP_CLASSPATH=\"s3n://utcs378/lap2456/jars/bdp-0.5.jar\"" >> /home/hadoop/conf/hadoop-user-env.sh