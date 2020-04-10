#!/usr/bin/env bash

pyinstaller -F kubesds-adm.py
pyinstaller -F kubesds-rpc.py
#pyinstaller -F kubesds-rpc-service.py

chmod +x ./dist/kubesds-adm ./dist/kubesds-rpc

#chmod +x ./dist/kubesds-rpc-service

#cp -f ./dist/kubesds-rpc-service /usr/bin

#cp -f ./dist/kubesds-adm ./dist/kubesds-rpc /usr/bin

cp -f ./dist/kubesds-adm /usr/bin

