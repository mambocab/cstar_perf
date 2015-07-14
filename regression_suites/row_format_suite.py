from cstar_perf.frontend.client.schedule import Scheduler
from cstar_perf.regression_suites.regression_suites import create_baseline_config

CSTAR_SERVER = "cstar.datastax.com"


def test_simple_profile(cluster='blade_11', load_rows=65000000, read_rows=65000000, threads=300):
    config = create_baseline_config()
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': ('write n={load_rows} -rate threads={threads} '
                     '-insert row-population-ratio=FIXED\(1\)/100 '
                     '-col n=FIXED\(600\)').format(
             load_rows=load_rows, threads=threads)},
        {'operation': 'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(read_rows=read_rows, threads=threads)},
        {'operation': 'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(read_rows=read_rows, threads=threads)}
    ]

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)
