#!/bin/bash
PIDS=`ps -ef | grep -i scrapy | grep second | awk '{print $2}' | xargs`

if [ -n "$PIDS" ]
then
	for pid in $PIDS
	do
		echo "Already run" $pid
	done
else
	echo "run scrapy"
	cd /home/junsoo/workspace/crawler
	scrapy crawl second --logfile=second 2>&1 > /home/junsoo/workspace/crawlerfrom_crontab &
fi
