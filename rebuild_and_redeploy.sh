#! /bin/bash
set -e
pid=$(pgrep -U $USER observer | awk '{print $1}')
pwd=$(pwd)
echo "current path: $pwd"

echo "kill the observer"
if [ -z "$pid" ]; then
    echo "observer is not running"
else
    echo "observer is running, pid: $pid"
    kill -9 $pid
fi

echo "rebuild the observer"
$pwd/build.sh debug --make

echo "redeploy the observer"
$pwd/tools/deploy/obd.sh start