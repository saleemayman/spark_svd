CASE=$1
DATA_DIR=$(dirname `pwd`)"/bodensee/eventlogs"
EVENT_FILES=${DATA_DIR}"/"${CASE}
OUTPUT_FILE=$2
#TEMP_FILE_1=$OUTPUT_FILE"_stages.csv"
#TEMP_FILE_2=$OUTPUT_FILE"_treeAggregateStages.csv"
echo $DATA_DIR

#grep 'Stage ID":.*Task End Reason":' $EVENT_FILES | ./tmp/bin/json2csv -k "Stage ID","Task Info"."Task ID","Task Info"."Index","Task Info"."Launch Time","Task Info"."Finish Time","Task Metrics"."Executor Run Time" > $TEMP_FILE_1 

#grep '"Event":"SparkListenerStageSubmitted".*"Stage Name":"treeAggregate.*"Number of Tasks":64' $EVENT_FILES | ./tmp/bin/json2csv -k "Stage Info"."Stage ID","Stage Info"."Stage Name","Stage Info"."Number of Tasks" | awk -F',' '{print $1","$3","substr($2, 0, 13)}' > $TEMP_FILE_2 
grep '"Event":"SparkListenerStageCompleted".*"Stage Name":"treeAggregate.*"Number of Tasks":' $EVENT_FILES | ./tmp/bin/json2csv -k "Stage Info"."Stage ID","Stage Info"."Stage Name","Stage Info"."Number of Tasks","Stage Info"."Submission Time","Stage Info"."Completion Time" | awk -F',' '{print $1","$3","$4","$5}' > $OUTPUT_FILE"_stageTimes.csv"

#awk -F',' 'FNR==NR {a[$1]; next} $1 in a' $TEMP_FILE_2 $TEMP_FILE_1 > $OUTPUT_FILE"_taskTimes.csv"

#rm $TEMP_FILE_1
#rm $TEMP_FILE_2
