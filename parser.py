import argparse
import storage

try:
    import xml.etree.CElementTree as ET
except:
    import xml.etree.ElementTree as ET


def parse_root(config_file):
    tree = ET.parse(config_file)
    root = tree.getroot()

    # --------------------------- cmd line parser ---------------------------------------
    parser = argparse.ArgumentParser(prog=root.attrib['prog'], description=root.attrib['description'])

    subparsers = parser.add_subparsers(help="sub-command help")

    storages = root.findall('storage')
    for storage in storages:
        classname = storage.attrib['class']

        # get all check config
        checks = storage.findall("check")
        for check in checks:
            parse_check(check, classname, subparsers)

        # get all operation config
        # ops = storage.findall("operation")
        # for op in ops:
        #     parse_operation(op, classname, subparsers)


def parse_check(check, classname, subparsers):
    check_info = {}
    errors = check.findall('error')
    for error in errors:
        check_info[error.attrib['return']] = error.attrib['description']
    errors = check.findall('error')
    for error in errors:
        check_info[error.attrib['return']] = error.attrib['description']


def parse_operation(op, classname, subparsers):
    # -------------------- add sub cmd ----------------------------------
    op_name = "%s%s" % (op.attrib['name'], classname)
    sub_parser = subparsers.add_parser(op_name, help="%s help" % op_name)

    args = op.findall('arg')
    args_check = []
    for arg in args:
        sub_parser.add_argument("--%s" % arg.attrib['name'], type=eval(arg.attrib['type']))
        args_check.append({'name': arg.attrib['name'], 'require': arg.attrib['require'], 'range': arg.attrib['range']})
    kinds = op.findall('kind')
    kind_funcs = {}
    for kind in kinds:
        kind_funcs[kind.attrib['type']] = []
        funcs = kind.findall('function')
        for func in funcs:
            kind_funcs[kind.attrib['type']].append(func.text)
    print args_check
    print kind_funcs



def executor(args_check, ):
    pass
    # if 'disk' == disk.attrib['device']:
    #     source_element = disk.find("source")
    #     if source_element.get("file") == source:
    #         source_element.set("file", target)
    #         tree.write('/tmp/%s.xml' % vm)


def createInstance(module_name, class_name, **kwargs):
  module_meta = __import__(module_name, globals(), locals(), [class_name])
  class_meta = getattr(module_meta, class_name)
  obj = class_meta(**kwargs)
  return obj

# p = createInstance('storage', 'Pool', pool='a')
# print p.is_virsh_active()

parse_root('cmd.xml')
