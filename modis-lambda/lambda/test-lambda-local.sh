#!/bin/bash
set -B
for i in {315..325};
do
 curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"myind": '$i'}';
done 
