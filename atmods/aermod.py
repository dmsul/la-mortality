from __future__ import division

import pandas as pd
import os
import shutil
import subprocess
import sys
import time
import tempfile

from atmods.env import (SCRATCH_PATH, AERMOD_SRC_PATH, PATH_FOR_MET,
                        AERMOD_OUTPUT_FILE, COMMAND)

# TODO: Check that passed DF columns are all kosher

PRECISION_SCALE = 1000

INP_TEMPLATE = (
    "CO STARTING\n"
    "   TITLEONE 'Processing firm {facid}'\n"
    "   MODELOPT CONC FLAT\n"
    "   AVERTIME MONTH\n"
    "   POLLUTID NOX\n"
    "   URBANOPT {pop1990}\n"
    "   RUNORNOT RUN\n"
    "   ERRORFIL aermod_errors.txt\n"
    "CO FINISHED\n"
    "SO STARTING\n"
    "   ELEVUNIT METERS\n"
    "{sources}\n"
    "   URBANSRC ALL\n"
    "   SRCGROUP ALL\n"
    "SO FINISHED\n"
    "RE STARTING\n"
    "   ELEVUNIT METERS\n"
    "{receptors}\n"
    "RE FINISHED\n"
    "ME STARTING\n"
    "   SURFFILE {met_file_path}{metsite_code}7.sfc\n"
    "   PROFFILE {met_file_path}{metsite_code}7.pfl\n"
    "   SURFDATA 99999 20{metsite_year}\n"
    "   UAIRDATA 99999 20{metsite_year}\n"
    "   PROFBASE {metsite_z} METERS\n"
    "   STARTEND 0{metsite_year} 01 01 0{metsite_year} 12 31\n"
    "ME FINISHED\n"
    "OU STARTING\n"
    "   POSTFILE MONTH ALL PLOT {aermod_output}\n"
    "   NOHEADER ALL\n"
    "OU FINISHED")

spaces3 = " " * 3
spaces17 = " " * 17

RECEPTOR_TEMPLATE = '\n   DISCCART {utm_east} {utm_north}'

STACK_TEMPLATE = (
    "\n   LOCATION STACK{id} POINT {utm_east} {utm_north}"
    "\n   SRCPARAM STACK{id} {emit_share} {stack_ht} {stack_temp}"
    " {stack_veloc} {stack_diam}")

POLAR_TEMPLATE = (spaces3 + "GRIDPOLR POL1 STA\n" +
                  spaces17 + "ORIG STACK1\n" +
                  spaces17 + "DIST {radii}\n" +
                  spaces17 + "GDIR {radial_bins} {deg_start} {deg_step}\n" +
                  spaces3 + "GRIDPOLR POL1 END")

STACKVARS = ['stack_ht', 'stack_temp', 'stack_veloc', 'stack_diam']


class Aermod(object):
    """
    Get Aermod exposure predictions for a list of receptors.

    Paramters
    ---------
    receptorDF : pandas DataFrame
        List of receptor locations. Coordinates must be named 'utm_east',
        'utm_north'.
    sourceDF : pandas DataFrame or Series
        A single source, potentially with several stacks. Each stack should
        have the following data (columns of DataFrame):
            'utm_east'
            'utm_north'
            'stack_ht', stack height in meters
            'stack_temp', stack exit gas temperature in Kelvin
            'stack_veloc', stack exit gas velocity in meters per second
            'stack_diam', stack diameter in meters
            'emit_share', stack's share of source's total emissions.
        This setup assumes that facility-level emissions data is being used
        with only a general idea of how much each stack contributes as a share
        of the total.

        WARNING: This is meant to be used with only one source at a time, but
        can support multiple firms in one DataFrame for testing purposes. But
        the meteorological data will be taken from the first source only.
    """

    def __init__(self, receptorDF, sourceDF, quiet=False, cleanup=True):

        self.receptorDF = receptorDF.copy()

        if isinstance(sourceDF, pd.DataFrame):
            self.sourceDF = sourceDF.copy()
        elif isinstance(sourceDF, pd.Series):
            self.sourceDF = pd.DataFrame(
                sourceDF.values.reshape(1, -1), columns=sourceDF.index
            ).copy()
        else:
            self.sourceDF = pd.DataFrame(sourceDF).copy()

        # Scale up `emit_share` for decimal precision
        self.sourceDF['emit_share'] *= PRECISION_SCALE

        self.quiet = quiet
        self.cleanup = cleanup

    def runModel(self):
        self.instance_path = self.prep_aermod_directory()
        inp_contents, unit_count = self.make_inp_file()
        with open(os.path.join(self.instance_path, 'aermod.inp'), 'w') as f:
            f.write(inp_contents)
        self.call_aermod(unit_count)
        # Format output
        output = self.read_output()
        output['exposure'] /= PRECISION_SCALE  # Re-scale exposure
        # Clean up temp folder
        if self.cleanup:
            try:
                shutil.rmtree(self.instance_path)
            except OSError:
                print "Err deleting tmp folder: {}".format(self.instance_path)
        else:
            print "Instance at {}".format(self.instance_path)

        return output

    def prep_aermod_directory(self):
        try:
            inst_path = tempfile.mkdtemp(dir=SCRATCH_PATH)
        except:
            print 'Scratch directory creation failed!'
            raise
        try:
            shutil.copy2(AERMOD_SRC_PATH, inst_path)
        except:
            print 'Aermod source copy failed!'
            raise
        return inst_path

    def make_inp_file(self):

        inp_variables = self.sourceDF.iloc[0, :].to_dict()

        # Receptors
        receptor_list = [RECEPTOR_TEMPLATE.format(**row.to_dict())
                         for idx, row in self.receptorDF.iterrows()]
        inp_variables['receptors'] = ''.join(receptor_list)

        # Sources
        source_list = []    # Gather all the strings
        source_id = 1       # To name the stacks uniquely
        for idx, sourcerow in self.sourceDF.iterrows():
            source_list.append(
                STACK_TEMPLATE.format(id=source_id, **sourcerow.to_dict()))
            source_id += 1
        inp_variables['sources'] = ''.join(source_list)

        # Metfiles
        inp_variables['met_file_path'] = PATH_FOR_MET
        inp_variables['aermod_output'] = AERMOD_OUTPUT_FILE

        unit_count = len(source_list) * len(receptor_list)
        return INP_TEMPLATE.format(**inp_variables), unit_count

    def call_aermod(self, unit_count=0):
        start = time.time()
        working_dir = os.getcwd()
        os.chdir(self.instance_path)
        try:
            if self.quiet:
                with open(os.devnull, 'w') as fnull:
                    subprocess.call(COMMAND, stdout=fnull)
            else:
                subprocess.call(COMMAND)
        except KeyboardInterrupt:
            os.chdir(working_dir)
            raise
        except:
            print "Aermod failed unexpectedly: ", sys.exc_info()[0]
            sys.exit(1)
        finally:
            os.chdir(working_dir)
        end = time.time()
        if unit_count > 0:
            time_per_unit = (end - start) / unit_count
            print "{} units, {:.4f} sec per unit".format(unit_count,
                                                         time_per_unit)
            sys.stdout.flush()

    def read_output(self):
        aermod_results_path = os.path.join(self.instance_path,
                                           AERMOD_OUTPUT_FILE)

        if os.path.getsize(aermod_results_path) < 1:
            raise IOError('Aermod did not run! Output is Empty!')

        columns = ['utm_east', 'utm_north', 'exposure',
                   'crap1',     # ex: 22.00
                   'crap2',     # ex: 22.00
                   'crap3',     # ex: 0.00
                   'crap4',     # ex: MONTH
                   'crap5',     # ex: ALL
                   'rawdate']
        usecols = ['utm_east', 'utm_north', 'exposure', 'rawdate']
        raw_output = pd.read_table(aermod_results_path,
                                   sep='\s*', engine='python',
                                   names=columns, usecols=usecols)

        # Time unit for reported averages set in 'aermod.inp' file
        raw_output['month'] = raw_output['rawdate'].apply(
            lambda x: int(str(x)[-6:-4])
        )
        del raw_output['rawdate']

        return raw_output
