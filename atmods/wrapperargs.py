import argparse


def Skeleton(parents, desc='', args=[],
             conflict_handler='error', add_help=True):
    parser = argparse.ArgumentParser(
        parents=parents, add_help=add_help, conflict_handler=conflict_handler,
        description=desc)

    if args:
        results = parser.parse_args(args=args)
    else:
        results = parser.parse_args()

    return vars(results)


def ModelArgs(**kwargs):
    desc = "Create exposure data locally."
    return Skeleton([common_opts, model_opts], description=desc, **kwargs)


def Sbatch(**kwargs):
    return Skeleton([common_opts, sbatch_opts], **kwargs)


# Set up base
common_opts = argparse.ArgumentParser(add_help=False)
common_opts.add_argument('geounit',
                         choices=['house', 'monitor', 'block', 'grid'],
                         help='Geographic unit for model')
common_opts.add_argument('model', help='Dispersion model')
common_opts.add_argument('--overwrite', action='store_true')
common_opts.add_argument('--quiet', action='store_true')
common_opts.add_argument('--altmaxdist', action='store_true',
                         help="Run 'altmaxdist' batch",)

# Parser for model runs
model_opts = argparse.ArgumentParser(add_help=False)
model_opts.add_argument('--chunk-info', type=int, nargs=2, default=None,
                        help="Chunk id, Total Chunks")


# sbatch-specific args
sbatch_opts = argparse.ArgumentParser(add_help=False)
# Designate firmlist
sbatch_opts.add_argument('--facids', type=int, nargs='+')
# Sbatch options
sbatch_opts.add_argument('-t', '--timescale', type=float, default=1,
                         help="Scale default run time")
sbatch_opts.add_argument('-p', '--partition',
                         choices=['interact', 'general', 'serial_requeue'],
                         default='serial_requeue',
                         help='Choose partition')
sbatch_opts.add_argument('--mail', choices=['all', 'end', 'start'],
                         action='append', default=['fail'])
sbatch_opts.add_argument('--yes', action='store_true',
                         help="Submit to Slurm w/o confirmation")
