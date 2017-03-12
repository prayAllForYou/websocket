#!/bin/bash

FWDIR="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"
source $FWDIR/bin/env.sh

echo "stop api server ... "

pidfile=$PID/pid.cloud.out
pid=`cat $pidfile`

if [ -f "$pidfile" ]; then
	if kill -0 $pid > /dev/null 2>&1; then
		echo "kill main process failed"
	fi
fi

for pid in `ps aux | grep "wsserver.py" | grep -v "grep" | grep $FWDIR | awk '{print $2}'`
do
    if kill -0 $pid > /dev/null 2>&1; then
        kill -9 $pid
    fi
done
