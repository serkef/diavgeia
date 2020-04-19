#/bin/bash

if [[ -z "$EXPORT_DIR" ]]; then
  echo "Please set EXPORT_DIR variable."
  exit 1
fi

cd $EXPORT_DIR
find . -maxdepth 1 -mindepth 1 -type d -exec tar -czf {}.tar.gz {} \; -exec rm -r {} \;
