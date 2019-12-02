'''
Copyright (2019, ) Institute of Software, Chinese Academy of Sciences

@author: liuhe18@otcaix.iscas.ac.cn
'''
import kubernetes

'''
Import python libs
'''
import os
import ConfigParser
import time
from threading import Thread


'''
Import third party libs
'''
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

'''
Import local libs
'''
# sys.path.append('%s/utils' % (os.path.dirname(os.path.realpath(__file__))))
from utils import logger


class parser(ConfigParser.ConfigParser):
    def __init__(self,defaults=None):
        ConfigParser.ConfigParser.__init__(self,defaults=None)
    def optionxform(self, optionstr):
        return optionstr

cfg = "%s/default.cfg" % os.path.dirname(os.path.realpath(__file__))
print cfg
config_raw = parser()
config_raw.read(cfg)

TOKEN = config_raw.get('Kubernetes', 'token_file')
PLURAL_VM = config_raw.get('VirtualMachine', 'plural')
VERSION_VM = config_raw.get('VirtualMachine', 'version')
GROUP_VM = config_raw.get('VirtualMachine', 'group')


TIMEOUT = config_raw.get('WatcherTimeout', 'timeout')

LOG = '/var/log/vnclet.log'

logger = logger.set_logger(os.path.basename(__file__), LOG)


TOKEN_PATH = os.getenv('TOKEN_PATH')

def main():
    logger.debug("---------------------------------------------------------------------------------")
    logger.debug("------------------------Welcome to Virtlet Daemon.-------------------------------")
    logger.debug("------Copyright (2019, ) Institute of Software, Chinese Academy of Sciences------")
    logger.debug("---------author: wuyuewen@otcaix.iscas.ac.cn,liuhe18@otcaix.iscas.ac.cn----------")
    logger.debug("--------------------------------wuheng@otcaix.iscas.ac.cn------------------------")
    logger.debug("---------------------------------------------------------------------------------")

    logger.debug("Loading kube token in 'default.cfg' ...")
    try:
        thread_1 = Thread(target=write_token_to_file)
        thread_1.daemon = True
        thread_1.name = 'write_token_to_file'
        thread_1.start()

        try:
            while True:
                time.sleep(3)
        except KeyboardInterrupt:
            return
    except:
        logger.error('Oops! ', exc_info=1)



# get all token every five seconds
def get_all_node_ip(group=GROUP_VM, version=VERSION_VM, plural=PLURAL_VM):
    all_node_ip = []
    configuration = kubernetes.client.Configuration()
    api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))

    try:
        jsondict = api_instance.list_node().to_dict()
        nodes = jsondict['items']
        for node in nodes:
            node_ip = {}
            for address in node['status']['addresses']:
                if address['type'] == 'InternalIP':
                    node_ip['ip'] = address['address']
                    break
            node_ip['nodeName'] = node['metadata']['name']
            all_node_ip.append(node_ip)

    except ApiException as e:
        print("Exception when calling CoreV1Api->list_node: %s\n" % e)
    except Exception as e:
        print("Exception when calling get_all_node_ip: %s\n" % e)

    return all_node_ip

# get all token every five seconds
def get_all_token(group=GROUP_VM, version=VERSION_VM, plural=PLURAL_VM):
    all_token = []
    try:
        jsondict = client.CustomObjectsApi().list_cluster_custom_object(group=group,
                                                                        version=version,
                                                                        plural=plural)
        domains = jsondict['items']
        for domain in domains:
            token = {}
            token['name'] = domain['metadata']['name']
            token['nodeName'] = domain['spec']['nodeName']
            token['port'] = domain['spec']['domain']['devices']['graphics'][0]['_port']
            if token['port'] == '-1':
                all_token.append(token)
    except ApiException as e:
        print("Exception when calling CoreV1Api->list_node: %s\n" % e)
    except Exception as e:
        print("Exception when calling get_all_node_ip: %s\n" % e)

    return all_token

def write_token_to_file():
    try:
        while True:
            try:
                result = []
                all_node_ip = get_all_node_ip()
                all_token = get_all_token()
                for node_ip in all_node_ip:
                    for token in all_token:
                        if node_ip['nodeName'] == token['nodeName']:
                            result.append(token['name'] + ': ' + node_ip['ip'] + ':' + str(token['port']) + '\n')
                if TOKEN_PATH is None:
                    logger.debug("error: can not get token path, exit,,,,,,")
                    return
                file_dir = os.path.dirname(TOKEN_PATH)
                if not os.path.isdir(file_dir):
                    os.makedirs(file_dir)

                with open(TOKEN_PATH, "w") as f:
                    for linne in result:
                        f.write(linne)
            except Exception, e:
                print("Exception when calling write_token_to_file: %s\n" % e)
            time.sleep(3)
    except KeyboardInterrupt:
        return



if __name__ == '__main__':
    config.load_kube_config(config_file=TOKEN)
    main()