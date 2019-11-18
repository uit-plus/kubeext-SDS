# coding=utf-8
import re

import psutil


# 获取网卡名称和其ip地址，不包括回环
def get_netcard():
    netcard_info = []
    info = psutil.net_if_addrs()
    for k, v in info.items():
        for item in v:
            if item[0] == 2 and not item[1] == '127.0.0.1':
                netcard_info.append((k, item[1]))
    return netcard_info

# 获取网卡名称和其ip地址，不包括回环
def get_docker0_IP():
    info = psutil.net_if_addrs()
    for k, v in info.items():
        for item in v:
            if k == 'docker0' and re.match('^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}$', item[1]):
                return item[1]

    return None

# 获取网卡名称和其ip地址，不包括回环
def get_all_IP():
    netcard_info = []
    info = psutil.net_if_addrs()
    for k, v in info.items():
        for item in v:
            if re.match('^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}$', item[1]):
                netcard_info.append({k: item[1]})

    return netcard_info


if __name__ == '__main__':
    print get_all_IP()
