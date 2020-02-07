#!/bin/bash

for FILENAME in ./*.gz; do
    NUMBER=$(cat "${FILENAME}" | gzip -d | wc -l)
    FIRST=$(cat "${FILENAME}" | gzip -d| head -1 | jq .id)

    echo "${FILENAME}, ${NUMBER}, ${FIRST}"
done
