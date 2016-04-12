EXEC_CORES=$1
EXEC_MEM=$2
DATA_FILE=$3
DELIMITER=$4
RDD_PARTITIONS=$5
JOB_OUTPUT_FILE=$6

rm -f results/$JOB_OUTPUT_FILE.out
rm -f runlogs/$JOB_OUTPUT_FILE.log

SPARK_HOME=/opt/ibm/dashdb-spark/spark
JAR_FILE=target/scala-2.10/testsvdtaxi_2.10-1.0.jar

#SPARK_CONF="--driver-class-path $EXTRACLASSPATH --driver-memory 64g --executor-memory 64g --conf spark.cores.max=$4 --conf spark.executor.cores=$5 --conf spark.default.parallelism=$6 --conf spark.executor.extraClassPath=$EXTRACLASSPATH"

echo "Starting Spark job: test_svd ..."
echo "Run command:"
echo "spark-submit --class test_svd --master spark://konstanz:7077 --deploy-mode client --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM $JAR_FILE $DATA_FILE $DELIMITER $RDD_PARTITIONS 10 > $JOB_OUTPUT_FILE"

$SPARK_HOME/bin/./spark-submit --class test_svd --master spark://konstanz:7077 --deploy-mode client --conf spark.cores.max=$EXEC_CORES --conf spark.task.cpus=2 --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM $JAR_FILE $DATA_FILE $DELIMITER $RDD_PARTITIONS 10 > results/$JOB_OUTPUT_FILE.out

mv run_logs_spark.log runlogs/$JOB_OUTPUT_FILE.log
mv eventlogs/app* eventlogs/$JOB_OUTPUT_FILE

echo "Job complete!"
echo "* * * * * Results * * * * *"
cat results/$JOB_OUTPUT_FILE.out

