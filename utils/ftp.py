import os
from ftplib import FTP, error_perm


def list_ftp_all_file(host, port, username, password):
    ftp = FTP()
    # ftp.set_debuglevel(2)
    ftp.connect("192.168.137.20", "21")
    ftp.login("ftpuser", "onceas")
    # print ftp.getwelcome()
    files = ftp.nlst()


    # bufsize = 1024
    # filename = "filename.txt"
    # file_handle = open(filename, "wb").write
    # ftp.retrbinary("RETR filename.txt", file_handle, bufsize)
    # ftp.set_debuglevel(0)
    ftp.quit()
    return files


def is_exist(path):
    ftp = FTP('10.35.1.86')  # my ftp
    try:
        ftp.login('thy', 'thy')
    except error_perm:
        print 'login error'

    # Suppose you want upload file to dir thy38
    try:
        ftp.cwd('thy38')
    except error_perm:
        try:
            ftp.mkd('thy38')
        except error_perm:
            print 'U have no authority to make dir'
    finally:
        ftp.quit()

    ftp.close()

def upload(file_path, target_path):
    ftp = FTP()
    # ftp.set_debuglevel(2)
    ftp.connect("192.168.137.20", "21")
    ftp.login("ftpuser", "onceas")
    # print ftp.getwelcome()
    dirname = os.path.dirname(target_path)
    filename = os.path.basename(target_path)
    ftp.cwd(dirname)
    files = ftp.nlst()
    if filename not in files:
        ftp.quit()
        return False
    ftp.quit()
    return True
    # bufsize = 1024
    # filename = "filename.txt"
    # file_handle = open(filename, "wb").write
    # ftp.retrbinary("RETR filename.txt", file_handle, bufsize)
    # ftp.set_debuglevel(0)