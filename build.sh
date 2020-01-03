#!/usr/bin/env bash

pyinstaller -F kubesds-adm.py
pyinstaller -F kubesds-rpc.py

chmod +x ./dist/kubesds-adm ./dist/kubesds-rpc

cp -f ./dist/kubesds-adm ./dist/kubesds-rpc /usr/bin
