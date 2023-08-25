#!/usr/bin/env bash

#  -v ./localhost-key.pem:/data/ssl/key.pem \
#  -v ./localhost-cert.pem:/data/ssl/cert.pem \
docker run -it --rm --name tls-mongo \
  -v $(pwd)/certs/localhost.pem:/data/ssl/key.pem \
  -v $(pwd)/tlsMongo.yaml:/etc/mongo/mongod.conf \
  mongo:7 --config /etc/mongo/mongod.conf
