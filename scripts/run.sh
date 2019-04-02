#!/bin/bash

# wait for the services to get ready
#
echo "*** Awaiting services to start Ingester ***"

while IFS=',' read -ra ADDR; do
  for i in "${ADDR[@]}"; do
      ./wait_for_service.sh "$i" "--timeout=0" "--stdout"
  done
done <<< $SERVICES_USED


# run the pipeline
#
echo "*** Starting Ingester ***"

python ingester --config config/config.yml
