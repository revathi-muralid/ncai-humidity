#!/bin/bash

docker build -t esds_jupyter .

docker run -it \
      --rm -p 8080:8888 \
        -v /home/ec2-user:/home/jovyan/work \
        -v /home/ec2-user/.aws:/home/jovyan/.aws:ro \
        esds_jupyter
