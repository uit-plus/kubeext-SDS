'''
Run back-end command in subprocess.
'''
import os
import random
import subprocess
from json import loads

import logger
from exception import ExecuteException

LOG = 'kubesds.log'

logger = logger.set_logger(os.path.basename(__file__), LOG)


def runCmdWithResult(cmd):
    if not cmd:
        return
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        std_out = p.stdout.readlines()
        std_err = p.stderr.readlines()
        if std_out:
            msg = ''
            for index, line in enumerate(std_out):
                if not str.strip(line):
                    continue
                msg = msg + str.strip(line)
            msg = str.strip(msg)
            try:
                result = loads(msg)
                return result
            except Exception:
                error_msg = ''
                for index, line in enumerate(std_err):
                    if not str.strip(line):
                        continue
                    error_msg = error_msg + str.strip(line)
                error_msg = str.strip(error_msg)
                raise ExecuteException('RunCmdError', 'can not parse cstor-cli output to json----'+msg+'. '+error_msg)
        if std_err:
            msg = ''
            for index, line in enumerate(std_err):
                if not str.strip(line):
                    continue
                if index == len(std_err) - 1:
                    msg = msg + str.strip(line) + '. ' + '***More details in %s***' % LOG
                else:
                    msg = msg + str.strip(line) + ', '
            raise ExecuteException('RunCmdError', msg)
    finally:
        p.stdout.close()
        p.stderr.close()


'''
Run back-end command in subprocess.
'''
def runCmdAndCheckReturnCode(cmd):
    if not cmd:
        logger.debug('No CMD to execute.')
        raise ExecuteException('error', 'cmd not found')

    result = ''
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        # print "error code", e.returncode, e.output
        # output = result.decode()
        # logger.debug(output)
        raise ExecuteException('ExecuteError', "Cmd: %s failed!" % cmd + ' cause: '+e.output)

def randomUUID():
    u = [random.randint(0, 255) for ignore in range(0, 16)]
    u[6] = (u[6] & 0x0F) | (4 << 4)
    u[8] = (u[8] & 0x3F) | (2 << 6)
    return "-".join(["%02x" * 4, "%02x" * 2, "%02x" * 2, "%02x" * 2,
                     "%02x" * 6]) % tuple(u)
