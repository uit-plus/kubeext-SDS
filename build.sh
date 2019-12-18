#!/usr/bin/env bash

pyinstaller -F kubesds-adm.py

chmod +x ./dist/kubesds-adm

cp -f ./dist/kubesds-adm /usr/bin
