#!/bin/bash

BUILD_TAG=$(git rev-parse HEAD)

currDay=2000-01-01
while [ "$currDay" != $(date -I) ]; do
  echo "Running for $currDay"
  currDay=$(date -I -d "$currDay + 1 day")
  docker run --rm \
  --name diavgeia-daily \
  --env-file=$HOME/diavgeia/diavgeia/.env \
  --volume=$HOME/diavgeia/logs:/home/app/logs \
  --volume=$HOME/.config/rclone/rclone.conf:/home/app/rclone.conf \
  diavgeia-daily:${BUILD_TAG} \
  --date $currDay
done
