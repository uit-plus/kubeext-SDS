import socket
from json import dumps

from kubernetes import client, config

import os, sys, ConfigParser

from kubernetes.client import V1DeleteOptions

from utils import logger


class parser(ConfigParser.ConfigParser):
    def __init__(self, defaults=None):
        ConfigParser.ConfigParser.__init__(self, defaults=None)

    def optionxform(self, optionstr):
        return optionstr


cfg = "/etc/kubevmm/config"
if not os.path.exists(cfg):
    cfg = "/home/kubevmm/bin/config"
config_raw = parser()
config_raw.read(cfg)

config.load_kube_config(config_file=config_raw.get('Kubernetes', 'token_file'))

resources = {}
for kind in ['VirtualMahcinePool', 'VirtualMachineDisk', 'VirtualMachineDiskImage', 'VirtualMachineDiskSnapshot']:
    resource = {}
    for key in ['version', 'group', 'plural']:
        resource[key] = config_raw.get(kind, key)
    resources[kind] = resource

logger = logger.set_logger(os.path.basename(__file__), '/var/log/kubesds.log')


def get(name, kind):
    jsondict = client.CustomObjectsApi().get_namespaced_custom_object(group=resources[kind]['group'],
                                                                      version=resources[kind]['version'],
                                                                      namespace='default',
                                                                      plural=resources[kind]['plural'],
                                                                      name=name)
    return jsondict


def create(name, data, kind):
    hostname = get_hostname_in_lower_case()
    jsondict = {'spec': {'volume': {}, 'nodeName': hostname, 'status': {}},
                'kind': kind, 'metadata': {'labels': {'host': hostname}, 'name': name},
                'apiVersion': '%s/%s' % (resources[kind]['group'], resources[kind]['version'])}

    jsondict = updateJsonRemoveLifecycle(jsondict, data)
    body = addPowerStatusMessage(jsondict, 'Ready', 'The resource is ready.')

    return client.CustomObjectsApi().create_namespaced_custom_object(
        group=resources[kind]['group'], version=resources[kind]['version'], namespace='default',
        plural=resources[kind]['plural'], body=body)


def update(name, data, kind):
    return client.CustomObjectsApi().replace_namespaced_custom_object(
        group=resources[kind]['group'], version=resources[kind]['version'], namespace='default', plural=resources[kind]['plural'], name=name, body=data)


def delete(name, data, kind):
    return client.CustomObjectsApi().delete_namespaced_custom_object(
        group=resources[kind]['group'], version=resources[kind]['version'], namespace='default', plural=resources[kind]['plural'], name=name, body=data)


def addPowerStatusMessage(jsondict, reason, message):
    if jsondict:
        status = {'conditions': {'state': {'waiting': {'message': message, 'reason': reason}}}}
        spec = get_spec(jsondict)
        if spec:
            spec['status'] = status
    return jsondict


def get_spec(jsondict):
    spec = jsondict.get('spec')
    if not spec:
        raw_object = jsondict.get('raw_object')
        if raw_object:
            spec = raw_object.get('spec')
    return spec


def deleteLifecycleInJson(jsondict):
    if jsondict:
        spec = get_spec(jsondict)
        if spec:
            lifecycle = spec.get('lifecycle')
            if lifecycle:
                del spec['lifecycle']
    return jsondict


def updateJsonRemoveLifecycle(jsondict, body):
    if jsondict:
        spec = get_spec(jsondict)
        if spec:
            lifecycle = spec.get('lifecycle')
            if lifecycle:
                del spec['lifecycle']
            spec.update(body)
    return jsondict


def get_hostname_in_lower_case():
    cfg = "/etc/kubevmm/config"
    if not os.path.exists(cfg):
        cfg = "/home/kubevmm/bin/config"
    config_raw = parser()
    config_raw.read(cfg)
    prefix = config_raw.get('Kubernetes', 'hostname_prefix')
    if prefix == 'vm':
        return 'vm.%s' % socket.gethostname().lower()
    else:
        return socket.gethostname().lower()

def changeNode(jsondict, newNodeName):
    if jsondict:
        jsondict['metadata']['labels']['host'] = newNodeName
        spec = get_spec(jsondict)
        if spec:
            nodeName = spec.get('nodeName')
            if nodeName:
                spec['nodeName'] = newNodeName
    return jsondict


class K8sHelper(object):
    def __init__(self, kind):
        self.kind = kind

    def get(self, name):
        try:
            jsondict = client.CustomObjectsApi().get_namespaced_custom_object(group=resources[self.kind]['group'],
                                                                              version=resources[self.kind]['version'],
                                                                              namespace='default',
                                                                              plural=resources[self.kind]['plural'],
                                                                              name=name)
            return jsondict
        except Exception:
            print dumps(
                {"result": {"code": 500, "msg": 'can not get %s %s on k8s.' % (self.kind, name)}, "data": {}})
            exit(3)

    def create(self, name, key, data):
        try:
            hostname = get_hostname_in_lower_case()
            jsondict = {'spec': {'volume': {}, 'nodeName': hostname, 'status': {}},
                        'kind': self.kind, 'metadata': {'labels': {'host': hostname}, 'name': name},
                        'apiVersion': '%s/%s' % (resources[self.kind]['group'], resources[self.kind]['version'])}

            jsondict = updateJsonRemoveLifecycle(jsondict, {key: data})
            body = addPowerStatusMessage(jsondict, 'Ready', 'The resource is ready.')

            return client.CustomObjectsApi().create_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], body=body)
        except Exception:
            print dumps(
                {"result": {"code": 500, "msg": 'can not create %s %s on k8s.' % (self.kind, name)}, "data": {}})
            exit(3)
    def update(self, name, key, data):
        try:
            jsondict = self.get(name)
            jsondict = updateJsonRemoveLifecycle(jsondict, {key: data})
            return client.CustomObjectsApi().replace_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], name=name, body=jsondict)
        except Exception:
            print dumps(
                {"result": {"code": 500, "msg": 'can not modify %s %s on k8s.' % (self.kind, name)}, "data": {}})
            exit(3)

    def delete(self, name):
        try:
            return client.CustomObjectsApi().delete_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], name=name, body=V1DeleteOptions())
        except Exception:
            print dumps(
                {"result": {"code": 500, "msg": 'can not delete %s %s on k8s.' % (self.kind, name)}, "data": {}})
            exit(3)

if __name__ == '__main__':
    k8s = K8sHelper('VirtualMachineDisk')
    disk1 = k8s.get('disk33333clone')
    print dumps(disk1)
    k8s.delete('disk33333clone1')
    k8s.create('disk33333clone1', 'volume', disk1['spec']['volume'])
    disk1['spec']['volume']['filename'] = 'lalalalalalala'
    k8s.update('disk33333clone1', 'volume', disk1['spec']['volume'])