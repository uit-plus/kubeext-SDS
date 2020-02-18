import os
import socket
from ftplib import FTP, error_perm

from exception import ExecuteException


def ftpconnect(host, port, username, password):
    ftp = FTP()
    # ftp.set_debuglevel(2)
    ftp.encoding = 'utf-8'
    try:
        ftp.connect(host, port)
        ftp.login(username, password)
    except(socket.error, socket.gaierror):
        raise ExecuteException('', 'can not connect ftp server.')
    except error_perm:
        raise ExecuteException('', 'can not connect ftp server.')
    return ftp


def dir(ftp, path):
    if not is_exist(ftp, path):
        raise ExecuteException('', 'not exist path %s on ftp server.' % path)
    ftp.cwd(path)
    files = ftp.nlst()
    return files


def mkdir(ftp, path):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(path)
    except error_perm:
        try:
            ftp.mkd(path)
        except error_perm:
            print 'U have no authority to make dir'


def is_exist(ftp, path):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(path)
    except error_perm:
        return False
    return True

def delete_file(ftp, path):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(os.path.dirname(path))
        ftp.delete(os.path.basename(path))
    except error_perm:
        return False
    return True


def uploadFile(ftp, files, target_path):
    try:
        if is_exist(ftp, target_path):
            ftp.cwd(target_path)
        else:
            ftp.mkd(target_path)
            ftp.cwd(target_path)

        for filename in files:
            bufsize = 1024
            file_handle = open(filename, "rb")
            ftp.storbinary("STOR %s" % os.path.basename(filename), file_handle, bufsize)
    except error_perm:
        raise ExecuteException('', 'error while upload file from ftp server. %s' % error_perm.message)



def downloadDir(ftp, download_path, target_path):
    if not os.path.exists(target_path):  # create file dir to save
        os.mkdir(target_path)
    try:
        if not is_exist(ftp, download_path):
            raise ExecuteException('', 'not exist file on ftp server which need to download ')
        ftp.cwd(download_path)
        files = dir(ftp, download_path)
        for filename in files:
            bufsize = 1024
            file_handle = open('%s/%s' % (target_path, filename), "wb")
            ftp.retrbinary("RETR %s" % filename, file_handle, bufsize)
    except error_perm:
        raise ExecuteException('', 'error while download file from ftp server. %s' % error_perm.message)


if __name__ == '__main__':
    ftp = ftpconnect('172.16.1.214', '21', 'ftpuser', 'ftpuser')
    ftp.set_debuglevel(2)
    uploadFile(ftp, ['/root/vmtest/vmtest.xml', '/root/vmtest/1.qcow2', '/root/vmtest/2.qcow2', '/root/vmtest/3.qcow2'], '/vmtest')
    # uploadFile(ftp, ['/root/vmtest/vmtest.xml', '/root/vmtest/1.qcow2', '/root/vmtest/2.qcow2', '/root/vmtest/3.qcow2'], '/uuid')
    # ftp.rmd('vmtest')
    # ftp.rename('uuid', 'vmtest')
    # downloadDir(ftp, '/vmtest', '/root/vmtest')
    ftp.set_debuglevel(0)