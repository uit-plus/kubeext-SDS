#!/usr/bin/env bash

pyinstaller -F --clean kubesds-adm.py

chmod +x ./dist/kubesds-adm

rm -f /usr/bin/kubesds-adm
cp -f ./dist/kubesds-adm /usr/bin
