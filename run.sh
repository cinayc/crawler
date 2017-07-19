#!/bin/bash
PID=`ps -ef | grep -i scrapy | grep for_parse_urls | awk '{print $2}'`

if [ $PID -gt 0 ]
then
	echo $PID
else
	echo none
fi
