#!/usr/bin/env bash

#  -v ./localhost-key.pem:/data/ssl/key.pem \
#  -v ./localhost-cert.pem:/data/ssl/cert.pem \
docker run --name tls-mongo \
  -v ./localhost.pem:/data/ssl/key.pem \
  -v ./tlsMongo.yaml:/etc/mongo/mongod.conf \
  -d \
  mongo:7 --config /etc/mongo/mongod.conf
