import imp
import os
import re
import tempfile


TINYDATA_BASE = '.tinydata'
SSL_CERT_LOCATION = '.ssl/my_cert.crt'
SSL_CACERTS_LOCATION = '.ssl/ca_certs.crt'
SSL_KEY_LOCATION = '.ssl/my_cert.key'
VALID_FILENAME_RE = re.compile('^[a-zA-Z0-9\-_\./]+$')

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


def load_map_reduce(contents):
    path = os.path.dirname(taskfile)
    taskmodulename = os.path.splitext(os.path.basename(taskfile))[0]
    logging.info("Loading task file %s from %s", taskmodulename, path)
    fp, pathname, description = imp.find_module(taskmodulename, [path])
    try:
        return imp.load_module(taskmodulename, fp, pathname, description)
    finally:
        if fp:
            fp.close()