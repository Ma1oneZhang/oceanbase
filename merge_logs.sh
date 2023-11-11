set -e
TRACE_ID=$1
LOG_PATH=/home/$USER/observer/log
MERGE_LOG_PATH=$2
# grep $TRACE_ID $LOG_PATH/observer* $LOG_PATH/election* $LOG_PATH/rootservice* > $MERGE_LOG_PATH
grep $TRACE_ID $LOG_PATH/observer* $LOG_PATH/election* $LOG_PATH/rootservice* | sed 's/:/ /' | awk '{tmp=$1; $1=$2; $2=$3; $3=$4; $4=tmp; print $0}' | sort > $MERGE_LOG_PATH