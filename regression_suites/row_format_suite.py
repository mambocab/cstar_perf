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


def test_on_disk_size(cluster='blade_11', load_rows='3M', read_rows='3M',
                      write_threads=10, read_threads=10):

    assert cluster in ('blade_11', 'blade_11b')

    config = create_baseline_config()
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': ('write n={load_rows} -rate threads={write_threads} '
                     '-insert row-population-ratio=FIXED\(1\)/100 '
                     '-col n=FIXED\(1000\)').format(load_rows=load_rows, write_threads=write_threads)},
        {'operation': 'nodetool',
         'command': 'cfstats keyspace1.standard1 -H',
         'nodes': 'blade-11-2a' if cluster == 'blade_11' else 'blade-11-7a'},
    ]
    for op in config['operations']:
        op['stress_revision'] = 'apache/trunk'

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)
