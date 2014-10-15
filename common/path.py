import os
import re

TINYDATA_BASE = '.tinydata'
SSL_CERT_LOCATION = '.ssl/my_cert.crt'
SSL_CACERTS_LOCATION = '.ssl/ca_certs.crt'
SSL_KEY_LOCATION = '.ssl/my_cert.key'
VALID_FILENAME_RE = re.compile('^[a-zA-Z0-9\-\_\./]+$')

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

def validate_filename(filename):
    match = VALID_FILENAME_RE.match(filename)
    return match and match.string is filename

def is_directory(filename):
    return os.path.isdir(get_filepath(filename))

def is_file(filename):
    return os.path.isfile(get_filepath(filename))

def mkdir(filename):
    if not is_file(filename) and not is_directory(filename):
        os.mkdir(get_filepath(filename))

def ls(filename):
    if is_file(filename):
        return [filename]
    elif is_directory(filename):
        return [os.path.join(filename, name) for name in os.listdir(get_filepath(filename))]
    else:
        return []