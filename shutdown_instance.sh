#! /bin/bash
set -e
pid=$(pgrep -U $USER observer)
if [ -z "$pid" ]; then
    echo "observer is not running "
    exit 1
else
    echo "Killing process $pid, which is observer"
    kill -9 $pid
fi