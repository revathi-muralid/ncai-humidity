#!/bin/bash

docker build -t esds_jupyter .

docker run -it \
      --rm -p 8080:8888 \
        -v "${PWD}"/../../:/home/jovyan/work \
        -v $HOME/.aws/credentials:/home/jovyan/.aws/credentials:ro \
        esds_jupyter
