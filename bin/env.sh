#!/bin/bash

WS_HOME="$(cd `dirname "${BASH_SOURCE-$0}"`/..; pwd)"

export WS_HOME=$WS_HOME
export LOGS=$WS_HOME/logs
export CONF=$WS_HOME/conf
export BIN=$WS_HOME/bin
export PID=$WS_HOME/pid
export LIB=$WS_HOME/lib
export SRC=$WS_HOME/src
export DATA=$WS_HOME/data
export TEMPLATES=$WS_HOME/templates
export SERVER=$SRC/server
export LOCALE=$WS_HOME/locale
export TASK=$SRC/task
export SCRIPT=$SRC/script
export TEST_RESOURCE=$WS_HOME/test/resource

# export python path
export PYTHONPATH=$LIB:$LIB/pyhs2:$LIB/push:$LIB/dateutil:$SRC:$SRC/common/cloghandler
#export PYTHON=/usr/bin/python
export PYTHON=python
export DATE=`date +"%Y-%m-%d"`

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${ORACLE_HOME}/lib
