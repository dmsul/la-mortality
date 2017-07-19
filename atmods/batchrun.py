import sys
import subprocess
from os.path import expanduser
from time import sleep

import numpy as np

from econtools import confirmer

from atmods.io import normed_firmexp_fname, parse_firm_info
from atmods.wrapperargs import Sbatch
from atmods.chunktools import calc_resources, calc_aermod_units
from atmods.check_jobs import jobs_to_run
from atmods.run_and_write import run_and_write


def main():
    args = Sbatch()

    geounit = args.pop('geounit')
    model = args.pop('model')
    facid_list = args.pop('facids', None)

    if model == 'aermod':
        batch_aermod(geounit, firm_list=facid_list, **args)
    else:
        batch_kernel(geounit, model, facid_list, **args)


def batch_aermod(geounit, firm_list=None, **kwargs):
    MODEL = 'aermod'
    bypass_confirm = kwargs.pop('yes')
    altmaxdist = kwargs.get('altmaxdist')

    units = calc_aermod_units(geounit, altmaxdist=altmaxdist)
    jobs_needed = jobs_to_run(geounit, MODEL, units=units,
                              cli_firm_list=firm_list, altmaxdist=altmaxdist)
    master_job_list = calc_resources(units, cli_facid_list=firm_list,
                                     altmaxdist=altmaxdist)

    scripts_to_submit = []
    for facid, firms_res in master_job_list.iterrows():
        num_chunks = int(firms_res['num_chunks'])
        for chunk_id in xrange(1, num_chunks + 1):
            chunk_id = int(chunk_id)
            # Pass the temp `firm_id` instead of `facid` for `jobname`
            jobname = normed_firmexp_fname(geounit, MODEL,
                                           int(firms_res['firm_id']),
                                           chunk_info=(chunk_id, num_chunks))
            if jobname in jobs_needed:
                scripts_to_submit.append(
                    sbatch_script(geounit, facid, jobname, firms_res, **kwargs)
                )

    job_master(scripts_to_submit, bypass_confirm=bypass_confirm)


def sbatch_script(geounit, facid, jobname, resources, **kwargs):
    """
    Create an Sbatch script to run `atmods.run_and_write` for a single
    firm-chunk (characterized by `jobname`)
    """

    partition = kwargs.pop('partition', 'serial_requeue')
    mail = kwargs.pop('mail', ['none'])
    overwrite = kwargs.pop('overwrite', False)
    timescale = kwargs.pop('timescale', 1.)
    altmaxdist = kwargs.pop('altmaxdist', True)
    model = 'aermod'

    cmd_str = (
        "python -c "
        "\"from atmods.run_and_write import run_and_write; "
        "run_and_write('{geounit}', '{model}', {facid},"
        "{chunk_info}, {overwrite}, {altmaxdist})\""
    )

    SBATCH = (
        '#!/usr/bin/env bash\n'
        '#SBATCH --job-name {jobname}\n'
        '#SBATCH --output /n/home08/dsulivan/jobout/{jobname}.out\n'
        '#SBATCH --error /n/home08/dsulivan/jobout/{jobname}.err\n'
        '#SBATCH -p {partition}\n'
        '#SBATCH -n 1\n'
        '#SBATCH -t {time}\n'
        '#SBATCH --mem={mem}\n'
        '#SBATCH --mail-type={mail}\n'
        '#SBATCH --mail-user=dsulivan\n'
    ) + cmd_str

    __, chunk_id, num_chunks = parse_firm_info(jobname + '.p')

    time = _request_time(resources['cpu_per_stack'], timescale, geounit)
    mem = _request_ram(geounit)
    mail = _set_email_param(mail)

    script = SBATCH.format(
        jobname=jobname,
        partition=partition,
        time=time,
        mem=mem,
        mail=mail,
        geounit=geounit,
        model=model,
        facid=facid,
        chunk_info=(chunk_id, num_chunks),
        overwrite=overwrite,
        altmaxdist=altmaxdist,
    )

    return script

def _set_email_param(mail):
    if 'none' in mail:
        mail = 'NONE'
    elif 'all' in mail:
        mail = 'ALL'
    else:
        mail = ','.join([x.upper() for x in mail])

    return mail

def _request_time(cpu_per_stack, timescale, geounit):
    time = np.ceil(cpu_per_stack) * 1.5     # Add 50% buffer
    time = max(time, 10)                    # Prevent too small
    time = int(time * timescale)            # CLI buffer
    if geounit not in ('grid', 'house'):    # Coarser unit, rel. more overhead
        time *= 2
    return time

def _request_ram(geounit):
    if geounit == 'house':
        mem = 1200
    elif geounit == 'grid':
        mem = 3000
    else:
        mem = 1000
    return mem


def job_master(scripts_to_submit, bypass_confirm=False):
    """
    `scripts_to_submit` is a list of strings. Each string is a Slurm sbatch
    script for the associated firm-chunk's job.
    """
    num_jobs = len(scripts_to_submit)

    if num_jobs == 0:
        print "No jobs to run!"
        sys.exit(0)

    # Print sample sbatch script to stdout, get confirmation before submitting
    sbatch_example = scripts_to_submit[0]
    print sbatch_example
    if bypass_confirm:
        confirmed = True
    else:
        prompt_str = '\n\n>>> Submit this job ({} copies)?'.format(num_jobs)
        confirmed = confirmer(prompt_str)

    # (Maybe) Write 1st sbatch script to disk
    if not confirmed or bypass_confirm:
        _save_sbatch_script(sbatch_example)

    # If CLI confirmation was false, exit; else, submit all the jobs
    if not confirmed:
        sys.exit(0)
    else:
        while scripts_to_submit:
            _submit_a_job(scripts_to_submit.pop())
            sleep(1)

def _save_sbatch_script(sbatch_script):
    """ Write the sbatch script to disk """
    tmp_file = expanduser('~/research/poll-house/code/temp.sbatch')
    with open(tmp_file, 'w') as f:
        f.write(sbatch_script)
    print 'Sbatch written as temp.sbatch in case you want it later'

def _submit_a_job(script):
    """ Submit string `script` to Slurm's `sbatch` command. """
    p = subprocess.Popen('sbatch',
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    p_stdout = p.communicate(input=script)[0]
    print '>>> ' + p_stdout


def batch_kernel(geounit, model, facid_list, **kwargs):
    overwrite = kwargs.pop('overwrite')
    run_and_write(geounit, model, facid_list, overwrite=overwrite)


if __name__ == '__main__':
    main()
