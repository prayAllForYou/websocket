#!/bin/bash

FWDIR="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
source $FWDIR/bin/env.sh

pid=$PID/pid.cloud.out

if [ -f "$pid" ]; then
	if kill -0 `cat $pid` > /dev/null 2>&1; then
		echo mcserver api server running as process `cat $pid`.  Stop it first.
		exit 1
	fi
fi
start_log=$LOGS/ws_boot.$DATE.log
error_log=$LOGS/ws_boot.$DATE.error

echo "start api server ..."
nohup $PYTHON $SERVER/wsserver.py 1>$start_log 2>$error_log & 
echo $! > $pid
