import socket
from json import dumps

from kubernetes import client, config

import os, sys, ConfigParser
from sys import exit


from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from exception import ExecuteException


class parser(ConfigParser.ConfigParser):
    def __init__(self,defaults=None):
        ConfigParser.ConfigParser.__init__(self,defaults=None)
    def optionxform(self, optionstr):
        return optionstr


cfg = "/etc/kubevmm/config"
if not os.path.exists(cfg):
    cfg = "/home/kubevmm/bin/config"
config_raw = parser()
config_raw.read(cfg)

config.load_kube_config(config_file=config_raw.get('Kubernetes', 'token_file'))

resources = {}
for kind in ['VirtualMachine', 'VirtualMahcinePool', 'VirtualMachineDisk', 'VirtualMachineDiskImage', 'VirtualMachineDiskSnapshot']:
    resource = {}
    for key in ['version', 'group', 'plural']:
        resource[key] = config_raw.get(kind, key)
    resources[kind] = resource


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

def hasLifeCycle(jsondict):
    if jsondict:
        spec = get_spec(jsondict)
        if spec:
            lifecycle = spec.get('lifecycle')
            if lifecycle:
                return True
    return False

def removeLifecycle(jsondict):
    if jsondict:
        spec = get_spec(jsondict)
        if spec:
            lifecycle = spec.get('lifecycle')
            if lifecycle:
                del spec['lifecycle']
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

def get_node_name(jsondict):
    if jsondict:
        return jsondict['metadata']['labels']['host']
    return None


class K8sHelper(object):
    def __init__(self, kind):
        self.kind = kind

    def exist(self, name):
        try:
            jsondict = client.CustomObjectsApi().get_namespaced_custom_object(group=resources[self.kind]['group'],
                                                                              version=resources[self.kind]['version'],
                                                                              namespace='default',
                                                                              plural=resources[self.kind]['plural'],
                                                                              name=name)
            return True
        except Exception:
            return False

    def get(self, name):
        try:
            jsondict = client.CustomObjectsApi().get_namespaced_custom_object(group=resources[self.kind]['group'],
                                                                              version=resources[self.kind]['version'],
                                                                              namespace='default',
                                                                              plural=resources[self.kind]['plural'],
                                                                              name=name)
            return jsondict
        except Exception:
            raise ExecuteException('RunCmdError', 'can not get %s %s on k8s.' % (self.kind, name))

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
            raise ExecuteException('RunCmdError', 'can not get %s %s on k8s.' % (self.kind, name))

    def get_create_jsondict(self, name, key, data):
        hostname = get_hostname_in_lower_case()
        jsondict = {'spec': {'volume': {}, 'nodeName': hostname, 'status': {}},
                    'kind': self.kind, 'metadata': {'labels': {'host': hostname}, 'name': name},
                    'apiVersion': '%s/%s' % (resources[self.kind]['group'], resources[self.kind]['version'])}

        jsondict = updateJsonRemoveLifecycle(jsondict, {key: data})
        body = addPowerStatusMessage(jsondict, 'Ready', 'The resource is ready.')
        return body

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
            jsondict = addPowerStatusMessage(jsondict, 'Ready', 'The resource is ready.')
            jsondict = updateJsonRemoveLifecycle(jsondict, {key: data})
            return client.CustomObjectsApi().replace_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], name=name, body=jsondict)
        except Exception:
            raise ExecuteException('RunCmdError', 'can not modify %s %s on k8s.' % (self.kind, name))

    def updateAll(self, name, jsondict):
        try:
            jsondict = addPowerStatusMessage(jsondict, 'Ready', 'The resource is ready.')
            jsondict = deleteLifecycleInJson(jsondict)
            return client.CustomObjectsApi().replace_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], name=name, body=jsondict)
        except Exception:
            raise ExecuteException('RunCmdError', 'can not modify %s %s on k8s.' % (self.kind, name))

    def delete(self, name):
        try:
            return client.CustomObjectsApi().delete_namespaced_custom_object(
                group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                plural=resources[self.kind]['plural'], name=name, body=V1DeleteOptions())
        except ApiException, e:
            if e.reason == 'Not Found':
                return

    def delete_lifecycle(self, name):
        try:
            jsondict = self.get(name)
            if hasLifeCycle(jsondict):
                jsondict = addPowerStatusMessage(jsondict, 'Ready', 'The resource is ready.')
                jsondict = removeLifecycle(jsondict)
                return client.CustomObjectsApi().replace_namespaced_custom_object(
                    group=resources[self.kind]['group'], version=resources[self.kind]['version'], namespace='default',
                    plural=resources[self.kind]['plural'], name=name, body=jsondict)
        except Exception:
            raise ExecuteException('RunCmdError', 'can not delete lifecycle %s %s on k8s.' % (self.kind, name))

    def change_node(self, name, newNodeName):
        jsondict = self.get(name)
        if jsondict:
            jsondict = addPowerStatusMessage(jsondict, 'Ready', 'The resource is ready.')
            jsondict['metadata']['labels']['host'] = newNodeName
            spec = get_spec(jsondict)
            if spec:
                nodeName = spec.get('nodeName')
                if nodeName:
                    spec['nodeName'] = newNodeName
            self.updateAll(name, jsondict)


def error_print(code, msg, data=None):
    if data is None:
        print dumps({"result": {"code": code, "msg": msg}, "data": {}})
        exit(1)
    else:
        print dumps({"result": {"code": code, "msg": msg}, "data": data})
        exit(1)

# if __name__ == '__main__':
#     print get_all_node_ip()
#     get_pools_by_path('/var/lib/libvirt/cstor/1709accf174vccaced76b0dbfccdev/1709accf174vccaced76b0dbfccdev')
    # k8s = K8sHelper('VirtualMachineDisk')
    # disk1 = k8s.get('disk33333clone')
    # print dumps(disk1)
    # k8s.delete('disk33333clone1')
    # k8s.create('disk33333clone1', 'volume', disk1['spec']['volume'])
    # disk1['spec']['volume']['filename'] = 'lalalalalalala'
    # k8s.update('disk33333clone1', 'volume', disk1['spec']['volume'])
