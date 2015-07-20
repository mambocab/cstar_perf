import datetime
# from unittest import skip

from cstar_perf.frontend.client.schedule import Scheduler

CSTAR_SERVER = "cstar.datastax.com"

# tests we want:
# - confirm there's a larger difference between old and new when column names are long
# - mixture of datatypes, esp. longs and text
#   - text biased toward 10-char strings
# - confirm that sstable with data representing small # of columns is smaller than sstables representing large # of cols
# - test with extremes:
#   - e.g. mostly 5-10 columns populated per row, but a few have 100s


def create_baseline_config(cluster='blade_11', title_suffix=''):
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
    if title_suffix:
        config['title'] += ': ' + title_suffix

    assert cluster in ('blade_11', 'blade_11_b')
    config['cluster'] = cluster

    return config


# @skip('skipping standard test')
def test_on_disk_size(cluster='blade_11_b', load_rows='3M',
                      write_threads=10, read_threads=10):

    config = create_baseline_config(cluster=cluster)
    config['operations'] = [
        {'operation': 'stress',
         'command': ('write n={load_rows} -rate threads={write_threads} '
                     # '-insert row-population-ratio=FIXED\(1\)/100 '
                     '-col n=FIXED\(1000\)').format(load_rows=load_rows, write_threads=write_threads)},
        {'operation': 'nodetool',
         'command': 'cfstats keyspace1.standard1 -H',
         'nodes': ['blade-11-2a' if cluster == 'blade_11' else 'blade-11-7a']},
    ]
    for op in config['operations']:
        op['stress_revision'] = 'apache/trunk'

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


# @skip("doesn't work yet :(")
def long_column_names_test(cluster='blade_11_b', load_rows='2M', write_threads=10):
    config = create_baseline_config(title_suffix='long column names test', cluster=cluster)
    config['operations'] = [
        {'operation': 'stress',
         'command': ('user profile="https://raw.githubusercontent.com/mambocab/cstar_perf/row-format-tests/regression_suites/long_names.yaml" '
                     'n={load_rows} ops\(insert=1\) -rate threads={write_threads} '
                     ).format(load_rows=load_rows, write_threads=write_threads)},
        {'operation': 'nodetool',
         'command': 'cfstats keyspace1.standard1 -H',
         'nodes': ['blade-11-2a' if cluster == 'blade_11' else 'blade-11-7a']},
    ]
    for op in config['operations']:
        op['stress_revision'] = 'apache/trunk'

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)
