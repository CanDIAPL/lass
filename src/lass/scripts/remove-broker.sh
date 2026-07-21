#!/bin/sh
echo "Removing container: \c"
docker rm -f lassbroker
echo "Removing network: \c"
docker network rm lassnet
