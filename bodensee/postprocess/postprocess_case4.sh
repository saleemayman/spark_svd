CASE=$1
DATA_DIR=$(dirname `pwd`)"/results"
RESULT_FILES=${DATA_DIR}"/"${CASE}"_run*.out"
OUTPUT_FILE=$2

# get the position of the file-path name where the run number starts
PREFIX_SIZE1=${#DATA_DIR}+${#CASE}
PREFIX_SIZE1=$(($PREFIX_SIZE1 + 6))
PREFIX_SIZE2=$(expr length "spark.default.parallelism x")

# summarize results for case with varying number of partitions (fixed number of cores/exec)
echo ""
echo "run, default.parallelism, partitions" > $CASE
join <(grep 'spark.default.parallelism' $RESULT_FILES | awk -F':' -v var1=$PREFIX_SIZE1 -v var2=$PREFIX_SIZE2 '{print substr($1, var1, 2)", " substr($2, var2, 2)}' | sort) <(grep '2. RDD num. of Partitions' $RESULT_FILES | awk -F': ' -v var=$PREFIX_SIZE1 '{print substr($1, var, 2)", " $2}' | sort) -t$', ' >> $CASE

# consolidate results in a CSV file
join <(sort $CASE) <(grep 'SVD computed, time' $RESULT_FILES | awk -F': ' -v var=$PREFIX_SIZE1 '{print substr($1, var, 2) ", " $2}' | sort) -t$', ' | sort -t $',' -k 1,1 -V > $OUTPUT_FILE
