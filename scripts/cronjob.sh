#!/bin/bash

BUILD_TAG=$(git rev-parse HEAD)
CRON_CMD='1 0 * * *	docker rm diavgeia-daily; docker run --name diavgeia-daily --env-file='$HOME'/diavgeia/diavgeia/.env --restart on-failure diavgeia-daily:'${BUILD_TAG}' --date $(date -d "yesterday" -I)'


echo
echo "Current crontab"
crontab -l 2>/dev/null

echo
echo "Removing cron job"
crontab -l 2>/dev/null | grep -v 'docker run --name diavgeia-daily' | crontab -

echo
echo "New crontab"
crontab -l 2>/dev/null

echo
echo "Adding new cron job: "
echo "   ${CRON_CMD}"
(crontab -l 2>/dev/null; echo "${CRON_CMD}") | sort -u - | crontab -

echo
echo "Final crontab"
crontab -l 2>/dev/null
