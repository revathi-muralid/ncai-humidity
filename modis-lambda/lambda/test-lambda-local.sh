#!/bin/bash

curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
	-d '{"myind": 0}'