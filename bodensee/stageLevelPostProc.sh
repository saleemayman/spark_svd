CASE=$1
DATA_DIR=$(dirname `pwd`)"/bodensee/eventlogs/old_cases"
EVENT_FILES=${DATA_DIR}"/"${CASE}
OUTPUT_FILE=$2
TEMP_FILE_1=$OUTPUT_FILE"_tasks.csv"
TEMP_FILE_2=$OUTPUT_FILE"_treeAggregateTasks_1stStage.csv"
TEMP_FILE_3=$OUTPUT_FILE"_treeAggregateTasks_2ndStage.csv"
echo $DATA_DIR

printf "stageId, taskId, taskIdx, startTime, finishTime, getResultTime, deserializeTime, GCTime, serializeTime, runTime, shuffleReadTime[ns], shuffleWriteTime[ns] \n" > $TEMP_FILE_1
grep 'Stage ID":.*Task End Reason":' $EVENT_FILES | ./tmp/bin/json2csv -k "Stage ID","Task Info"."Task ID","Task Info"."Index","Task Info"."Launch Time","Task Info"."Finish Time","Task Info"."Getting Result Time","Task Metrics"."Executor Deserialize Time","Task Metrics"."JVM GC Time","Task Metrics"."Result Serialization Time","Task Metrics"."Executor Run Time","Task Metrics"."Shuffle Read Metrics"."Fetch Wait Time","Task Metrics"."Shuffle Write Metrics"."Shuffle Write Time" >> $TEMP_FILE_1 

grep '"Event":"SparkListenerStageSubmitted".*"Stage Name":"treeAggregate.*"Number of Tasks":8' $EVENT_FILES | ./tmp/bin/json2csv -k "Stage Info"."Stage ID","Stage Info"."Stage Name","Stage Info"."Number of Tasks" | awk -F',' '{print $1","$3","substr($2, 0, 13)}' > $TEMP_FILE_2 

grep '"Event":"SparkListenerStageSubmitted".*"Stage Name":"treeAggregate.*"Number of Tasks":2' $EVENT_FILES | ./tmp/bin/json2csv -k "Stage Info"."Stage ID","Stage Info"."Stage Name","Stage Info"."Number of Tasks" | awk -F',' '{print $1","$3","substr($2, 0, 13)}' > $TEMP_FILE_3 

awk -F',' 'FNR==NR {a[$1]; next} $1 in a' $TEMP_FILE_2 $TEMP_FILE_1 > $OUTPUT_FILE"_taskTimes_1stStage.csv"
awk -F',' 'FNR==NR {a[$1]; next} $1 in a' $TEMP_FILE_3 $TEMP_FILE_1 > $OUTPUT_FILE"_taskTimes_2ndStage.csv"

rm $TEMP_FILE_1
rm $TEMP_FILE_2
rm $TEMP_FILE_3
