import socket
from json import dumps

from kubernetes import client, config

import os, sys, configparser
from sys import exit


from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from utils import logger

config.load_kube_config(config_file="/root/.kube/config")

resources = {
    'VirtualMahcinePool': {'version': 'v1alpha3', 'group': 'cloudplus.io', 'plural': 'virtualmachinepools'},
    'VirtualMachineDisk': {'version': 'v1alpha3', 'group': 'cloudplus.io', 'plural': 'virtualmachinedisks'},
    'VirtualMachineDiskImage': {'version': 'v1alpha3', 'group': 'cloudplus.io', 'plural': 'virtualmachinediskimages'},
    'VirtualMachineDiskSnapshot': {'version': 'v1alpha3', 'group': 'cloudplus.io', 'plural': 'virtualmachinedisksnapshots'},
}

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
        group=resources[kind]['group'], version=resources[kind]['version'], namespace='default',
        plural=resources[kind]['plural'], name=name, body=data)


def delete(name, data, kind):
    return client.CustomObjectsApi().delete_namespaced_custom_object(
        group=resources[kind]['group'], version=resources[kind]['version'], namespace='default',
        plural=resources[kind]['plural'], name=name, body=data)


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
    return 'vm.%s' % socket.gethostname().lower()


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
            error_print(500, 'can not get %s %s on k8s.' % (self.kind, name))

    def get_data(self, name, key):
        try:
            jsondict = client.CustomObjectsApi().get_namespaced_custom_object(group=resources[self.kind]['group'],
                                                                              version=resources[self.kind]['version'],
                                                                              namespace='default',
                                                                              plural=resources[self.kind]['plural'],
                                                                              name=name)
            if 'spec' in jsondict.keys() and isinstance(jsondict['spec'], dict) and key in jsondict['spec'].keys():
                return jsondict['spec'][key]
            return None
        except Exception:
            error_print(500, 'can not get %s %s on k8s.' % (self.kind, name))

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
            error_print(500, 'can not create %s %s on k8s.' % (self.kind, name))

    def update(self, name, key, data):
        try:
            jsondict = self.get(name)
            jsondict = updateJsonRemoveLifecycle(jsondict, {key: data})
            return client.CustomObjectsApi().replace_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], name=name, body=jsondict)
        except Exception:
            error_print(500, 'can not modify %s %s on k8s.' % (self.kind, name))

    def delete(self, name):
        try:
            return client.CustomObjectsApi().delete_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], name=name, body=V1DeleteOptions())
        except ApiException as e:
            if e.reason == 'Not Found':
                logger.debug('**Object %s already deleted.' % name)
                return

def error_print(code, msg, data=None):
    if data is None:
        print(dumps({"result": {"code": code, "msg": msg}, "data": {}}))
        exit(1)
    else:
        print(dumps({"result": {"code": code, "msg": msg}, "data": data}))
        exit(1)

if __name__ == '__main__':
    k8s = K8sHelper('VirtualMachineDisk')
    disk1 = k8s.get('disk33333clone')
    print(disk1)
    k8s.delete('disk33333clone1')
    k8s.create('disk33333clone1', 'volume', disk1['spec']['volume'])
    disk1['spec']['volume']['filename'] = 'lalalalalalala'
    k8s.update('disk33333clone1', 'volume', disk1['spec']['volume'])
