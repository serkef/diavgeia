#!/bin/bash

echo "Running diavgeia fetch with args \`$@\`"
diavgeia-daily "$@"

echo "Uploading data to bucket"
rclone move $EXPORT_DIR $RCLONE_B2_REMOTE:$BUCKET_NAME/raw --config /home/app/rclone.conf --log-file=$LOG_DIR/rclone_$(date -u +"%Y-%m-%dT%H:%M:%SZ")
