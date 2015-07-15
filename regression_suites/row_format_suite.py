import datetime
from cstar_perf.frontend.client.schedule import Scheduler

CSTAR_SERVER = "cstar.datastax.com"


def create_baseline_config():
    """Creates a config for testing the latest dev build(s) against stable and oldstable"""

    dev_revisions = ['apache/trunk', 'apache/cassandra-2.2']

    config = {}

    config['revisions'] = revisions = []
    for r in dev_revisions:
        revisions.append({'revision': r, 'label': r + ' (dev)'})
    for r in revisions:
        r['options'] = {'use_vnodes': True}
        r['java_home'] = "~/fab/jvms/jdk1.8.0_45"

    config['title'] = 'Jenkins C* row format suite - {}'.format(datetime.datetime.now().strftime("%Y-%m-%d"))

    return config


def test_simple_profile(cluster='blade_11', load_rows='3M', read_rows='3M', threads=25):
    config = create_baseline_config()
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': ('write n={load_rows} -rate threads={threads} '
                     '-insert row-population-ratio=FIXED\(1\)/100 '
                     '-col n=FIXED\(600\)').format(
             load_rows=load_rows, threads=threads)},
        {'operation': 'nodetool', 'command': 'cfstats -H'},
        {'operation': 'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(read_rows=read_rows, threads=threads)},
        {'operation': 'stress',
         'command': 'read n={read_rows} -rate threads={threads}'.format(read_rows=read_rows, threads=threads)}
    ]
    for op in config['operations']:
        op['stress_revision'] = 'apache/trunk'

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)
