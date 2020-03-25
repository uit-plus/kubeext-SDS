import os
import socket
import traceback
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


def listdir(ftp, path):
    if not is_exist_dir(ftp, path):
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
def makedirs(ftp, path):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(path)
    except error_perm:
        try:
            if os.path.dirname(path) != '/':
                makedirs(ftp, os.path.dirname(path))
            ftp.cwd(os.path.dirname(path))
            ftp.mkd(os.path.basename(path))
        except error_perm:
            print 'U have no authority to make dir'

def rename(target, filename, newname):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(target)
        ftp.rename(filename, newname)
    except error_perm:
        return False
    return True

def is_exist_dir(ftp, path):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(path)
    except error_perm:
        return False
    return True

def is_exist_file(ftp, path):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(os.path.dirname(path))
        if os.path.basename(path) in ftp.nlst():
            return True
    except error_perm:
        return False
    return False

def delete_file(ftp, path):
    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd(os.path.dirname(path))
        ftp.delete(os.path.basename(path))
    except error_perm:
        return False
    return True


def upload_files(ftp, files, target_path):
    try:
        if is_exist_dir(ftp, target_path):
            ftp.cwd(target_path)
        else:
            makedirs(ftp, target_path)
            ftp.cwd(target_path)

        for filename in files:
            bufsize = 1024
            file_handle = open(filename, "rb")
            ftp.storbinary("STOR %s" % os.path.basename(filename), file_handle, bufsize)
    except error_perm:
        raise ExecuteException('', 'error while upload file from ftp server. %s' % error_perm.message)

def upload_file(ftp, file, target_dir):
    try:
        if is_exist_dir(ftp, target_dir):
            ftp.cwd(target_dir)
        else:
            makedirs(ftp, target_dir)
            ftp.cwd(target_dir)
        filename = os.path.basename(file)
        if filename in ftp.nlst():
            if '%s.bak' % filename in ftp.nlst():
                ftp.delete('%s.bak' % filename)
            ftp.rename(filename, '%s.bak' % filename)
        bufsize = 1024
        file_handle = open(file, "rb")
        ftp.storbinary("STOR %s" % os.path.basename(file), file_handle, bufsize)

    except error_perm:
        # traceback.print_exc()
        raise ExecuteException('', 'error while upload file from ftp server. %s' % error_perm.message)

def get_dir_files(source_file_dir):
    files = []
    for source_file in os.listdir(source_file_dir):
        if os.path.isfile('%s/%s' % (source_file_dir, source_file)):
            files.append('%s/%s' % (source_file_dir, source_file))
        elif os.path.isdir('%s/%s' % (source_file_dir, source_file)):
            files.extend(get_dir_files('%s/%s' % (source_file_dir, source_file)))
    return files

def upload_dir(ftp, source_file_dir, target):
    # print source_file_dir
    if not os.path.exists(source_file_dir):
        raise ExecuteException('', 'not exist source file dir.')

    if not is_exist_dir(ftp, target):
        makedirs(ftp, target)

    files = get_dir_files(source_file_dir)
    for file in files:
        if target != '/':
            target_path = file.replace(source_file_dir, target)
        else:
            target_path = file.replace(source_file_dir, '')
        if not is_exist_dir(ftp, os.path.dirname(target_path)):
            makedirs(ftp, os.path.dirname(target_path))
        upload_file(ftp, file, os.path.dirname(target_path))


def download_dir(ftp, download_path, target_path):
    if not os.path.exists(target_path):  # create file dir to save
        os.mkdir(target_path)
    try:
        if not is_exist_dir(ftp, download_path):
            raise ExecuteException('', 'not exist file on ftp server which need to download ')
        ftp.cwd(download_path)
        files = listdir(ftp, download_path)
        for filename in files:
            bufsize = 1024
            file_handle = open('%s/%s' % (target_path, filename), "wb")
            ftp.retrbinary("RETR %s" % filename, file_handle, bufsize)
    except error_perm:
        raise ExecuteException('', 'error while download file from ftp server. %s' % error_perm.message)

def download_file(ftp, download_path, target_path):
    if not os.path.exists(os.path.dirname(target_path)):  # create file dir to save
        os.mkdir(os.path.dirname(target_path))
    try:
        if not is_exist_file(ftp, download_path):
            raise ExecuteException('', 'not exist file on ftp server which need to download ')
        ftp.cwd(os.path.dirname(download_path))
        filename = os.path.basename(download_path)
        bufsize = 1024
        with open(target_path, 'wb') as fp:
            ftp.retrbinary("RETR %s" % filename, fp.write, bufsize)
    except error_perm:
        raise ExecuteException('', 'error while download file from ftp server. %s' % error_perm.message)


if __name__ == '__main__':
    ftp = ftpconnect('172.16.1.214', '21', 'ftpuser', 'ftpuser')
    ftp.set_debuglevel(2)
    # upload_file(ftp, '/tmp/123.json', '/vmbackuptest/clouddiskbackup/vmbackuptestdisk1')
    print listdir(ftp, '/vmbackuptest')
    # download_file(ftp, '/vmbackuptest/clouddiskbackup/vmbackuptestdisk1/history.json', '/tmp/123.json')
    # makedirs(ftp, '/test/test1/test2')
    # upload_dir(ftp, '/var/lib/libvirt/cstor/a639873f92a24a9ab840492f0e538f2b/a639873f92a24a9ab840492f0e538f2b/vmbackup', '/')
    # upload_file(ftp, ['/root/vmtest/vmtest.xml', '/root/vmtest/1.qcow2', '/root/vmtest/2.qcow2', '/root/vmtest/3.qcow2'], '/vmtest')
    # uploadFile(ftp, ['/root/vmtest/vmtest.xml', '/root/vmtest/1.qcow2', '/root/vmtest/2.qcow2', '/root/vmtest/3.qcow2'], '/uuid')
    # ftp.rmd('vmtest')
    # ftp.rename('uuid', 'vmtest')
    # downloadDir(ftp, '/vmtest', '/root/vmtest')
    # ftp.set_debuglevel(0)
    # print get_dir_files('/root/test1')