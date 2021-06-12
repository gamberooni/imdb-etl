#!/bin/bash

. $SPARK_HOME/sbin/spark-config.sh
. $SPARK_HOME/bin/load-spark-env.sh

mkdir -p $SPARK_WORKER_LOG 
# symlink stdout to log file
ln -sf /dev/stdout $SPARK_WORKER_LOG/spark-worker.out

# start worker
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
    $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out 
