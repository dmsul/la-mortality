from os import path
import socket
import datetime


def data_path(*args):
    return path.join(DATA_PATH, *args)


def out_path(*args):
    return path.join(OUT_PATH, *args)


def src_path(*args):
    return path.join(SRC_PATH, *args)


def test_path(*args):
    return path.join(TESTDATA_PATH, *args)


def bulk_path(*args):
    return path.join(BULK_DATA, *args)


home = path.expanduser('~')
project_root_from_home = path.normpath('research/la-mortality')
root_dir = path.join(home, project_root_from_home)

DATA_PATH = path.join(root_dir, 'data')
SRC_PATH = path.join(DATA_PATH, 'src')
TESTDATA_PATH = path.join(DATA_PATH, 'for_tests')

now = datetime.datetime.now()
out_month = str(now.year)[-2:] + str(now.month).zfill(2)
OUT_PATH = path.join(root_dir, 'out', out_month)

bulk_io = {
    'harvard': path.normpath('/n/regal/economics/dsulivan/'),
    'mine': DATA_PATH,
    'nberlocal': DATA_PATH,
    'nberserver': DATA_PATH,
}

HOST = socket.gethostname()
# Odyssey
if 'harvard' in HOST:
    hostname = 'harvard'
# Mine
elif HOST in ['ThinkPad-PC', 'Daniel-PC', 'DESKTOP-CHIVFBQ', 'sullivan-7d']:
    hostname = 'mine'
# NBER
elif HOST in ['admin-PC']:
    hostname = 'nberlocal'
elif 'nber' in HOST:
    hostname = 'nberserver'
else:
    # Harvard's serial_requeue has weird names sometimes, assume it's that
    hostname = 'harvard'

BULK_DATA = bulk_io[hostname]
