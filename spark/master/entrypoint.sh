#!/bin/bash

export SPARK_MASTER_HOST=`hostname`

. $SPARK_HOME/sbin/spark-config.sh
. $SPARK_HOME/bin/load-spark-env.sh

mkdir -p $SPARK_MASTER_LOG  
# symlink stdout to log file
ln -sf /dev/stdout $SPARK_MASTER_LOG/spark-master.out

# start master
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
    --host $SPARK_MASTER_HOST \
    --port $SPARK_MASTER_PORT \
    --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out 
