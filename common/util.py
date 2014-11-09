import imp
import os
import tempfile
import zlib


TINYDATA_BASE = '.tinydata'
SSL_CERT_LOCATION = '.ssl/my_cert.crt'
SSL_CACERTS_LOCATION = '.ssl/ca_certs.crt'
SSL_KEY_LOCATION = '.ssl/my_cert.key'

# all filenames are relative to the base (i.e. dataset1/data.csv and not just data.csv)


def get_tinydata_base():
    return os.path.join(os.environ['HOME'], TINYDATA_BASE)


def get_ssl_cert():
    return os.path.join(get_tinydata_base(), SSL_CERT_LOCATION)


def get_ssl_cacerts():
    return os.path.join(get_tinydata_base(), SSL_CACERTS_LOCATION)


def get_ssl_key():
    return os.path.join(get_tinydata_base(), SSL_KEY_LOCATION)


def get_filepath(filename):
    return os.path.join(get_tinydata_base(), filename)


def load_module(module_path):
    module_dir = os.path.dirname(module_path)
    module_name = os.path.splitext(os.path.basename(module_path))[0]
    fp, pathname, description = imp.find_module(module_name, [module_dir])
    try:
        return imp.load_module(module_name, fp, pathname, description)
    finally:
        if fp:
            fp.close()


def serialize_module(module_path):
    with open(module_path, 'r') as module_file:
        return zlib.compress(module_file.readlines())


def deserialize_module(module_contents):
    module_contents = zlib.decompress(module_contents)
    fd, module_path = tempfile.mkstemp(".py", "tinydata", text=True)
    try:
        os.write(fd, module_contents)
        module = load_module(module_path)
    finally:
        os.close(fd)
        os.remove(module_path)
        return module


class ReturnStatus(object):
    FAIL = 0
    SUCCESS = 1
