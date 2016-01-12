CASE=$1
DATA_DIR=$(dirname `pwd`)"/results"
RESULT_FILES=${DATA_DIR}"/"${CASE}"_run*.out"
OUTPUT_FILE=$2

# get the position of the file-path name where the run number starts
PREFIX_SIZE=${#DATA_DIR}+${#CASE}
PREFIX_SIZE=$(($PREFIX_SIZE + 6))

# summarize results for case with varying number of partitions (fixed number of cores/exec)
echo ""
echo "run, partitions" > $CASE
grep '2. RDD num. of Partitions' $RESULT_FILES | awk -F': ' -v var=$PREFIX_SIZE '{print substr($1, var, 2)", " $2}' | sort >> $CASE

# consolidate results in a CSV file
#join <(sort $CASE) <(grep 'SVD computed, time' $RESULT_FILES | awk -F': ' -v var=$PREFIX_SIZE '{print substr($1, var, 2) ", " $2}' | sort) -t$', ' | sort -t $',' -k 2,2 -V
join <(sort $CASE) <(grep 'SVD computed, time' $RESULT_FILES | awk -F': ' -v var=$PREFIX_SIZE '{print substr($1, var, 2) ", " $2}' | sort) -t$', ' | sort -t $',' -k 2,2 -V > $OUTPUT_FILE
