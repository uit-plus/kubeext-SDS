#coding=utf-8

import uuid

from utils.utils import runCmd

runCmd('kubesds-adm createPool  --opt nolock --url 192.168.96.250:/home/nfs --content vmd   --type nfs --pool nfspoola1a')

