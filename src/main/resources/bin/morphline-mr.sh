#!/bin/sh
hadoop jar morphline-mr.jar -f morphline.conf -m morphline1 -i $1 -o $2