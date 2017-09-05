import os
import socket

# AERMOD paths
HOST = socket.gethostname()
if HOST in ['ThinkPad-PC', 'Daniel-PC', 'DESKTOP-CHIVFBQ', 'sullivan-7d']:
    AERMOD_ROOT = os.path.normpath('d:/data/pollution/aermod/')
    BIN_NAME = 'aermod.exe'
    SCRATCH_PATH = os.path.normpath('d:/data/pollution/aermod_inst')
    COMMAND = 'aermod'
else:
    AERMOD_ROOT = os.path.normpath('/n/home08/dsulivan/epa_atmos/aermod/')
    BIN_NAME = 'aermod_unix_orig'
    # BIN_NAME = 'aermod_faster'
    SCRATCH_PATH = os.path.normpath('/scratch')
    COMMAND = os.path.join('.', BIN_NAME)
AERMOD_SRC_PATH = os.path.join(AERMOD_ROOT, BIN_NAME)
PATH_FOR_MET = os.path.join(AERMOD_ROOT, os.path.normpath('met')) + os.sep
AERMOD_OUTPUT_FILE = 'this_aermod_out.txt'

MAXDIST = 20
ALTMAXDIST = 30

# CPU speed
JOB_LIMIT_MIN = 210.
CPU_SEC_PER_UNIT = 0.065

FIRMS_FOR_ALTMAXDIST = (
    800089,     # Just west of County
    800144,     # Between County and Far West [confirmed]
    # 800319,     # Far West (west to east lines)
    # 131003,
    # 800223,
    # 800026,
    # 47232,
    # 115314,
)
