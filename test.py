import random
import shlex
import subprocess
import traceback


# def run_command(command):
#     process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
#     while True:
#         output = process.stdout.readline()
#         if output == '' and process.poll() is not None:
#             break
#         if output:
#             print output.strip()
#     rc = process.poll()
#     return rc
#
# run_command('ping www.baidu.com -t')

def randomUUID():
    u = [random.randint(0, 255) for ignore in range(0, 16)]
    u[6] = (u[6] & 0x0F) | (4 << 4)
    u[8] = (u[8] & 0x3F) | (2 << 6)
    return "-".join(["%02x" * 4, "%02x" * 2, "%02x" * 2, "%02x" * 2,
                     "%02x" * 6]) % tuple(u)


if __name__ == '__main__':
    for i in range(100):
        print randomUUID().replace('-', '')