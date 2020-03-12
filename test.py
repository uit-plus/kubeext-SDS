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

p = subprocess.Popen('ping www.baidu.com', shell=True, stdout=subprocess.PIPE)
try:
    while True:
        output = p.stdout.readline()
        if output == '' and p.poll() is not None:
            break
        if output:
            # print output.strip()
            p.terminate()
except Exception:
    traceback.print_exc()
finally:
    p.stdout.close()
