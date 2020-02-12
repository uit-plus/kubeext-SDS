#!/usr/bin/env bash

pyinstaller -F kubesds-adm.py
pyinstaller -F kubesds-rpc.py
pyinstaller -F kubesds-rpc-service.py

chmod +x ./dist/kubesds-adm ./dist/kubesds-rpc ./dist/kubesds-rpc-service

cp -f ./dist/kubesds-adm ./dist/kubesds-rpc ./dist/kubesds-rpc-service /usr/bin
