import datetime
from util import get_tagged_releases
from cstar_perf.frontend.client.schedule import Scheduler

CSTAR_SERVER = "cstar.datastax.com"


def create_baseline_config(title=None, revision_config_options=None):
    """Creates a config for testing the latest dev build(s) against stable and oldstable"""
    if revision_config_options is None:
        revision_config_options = {}

    dev_revisions = ['apache/trunk']
    stable = get_tagged_releases('stable')[0]
    oldstable = get_tagged_releases('oldstable')[0]

    config = {}

    config['revisions'] = revisions = []
    for r in dev_revisions:
        revisions.append({'revision': r, 'label': r + ' (dev)'})
    revisions.append({'revision': stable, 'label': stable + ' (stable)'})
    revisions.append({'revision': oldstable, 'label': oldstable+' (oldstable)'})
    for r in revisions:
        r['options'] = {'use_vnodes': True}
        r['java_home'] = "~/fab/jvms/jdk1.7.0_71" if 'oldstable' in r['label'] else "~/fab/jvms/jdk1.8.0_45"
        r.update(revision_config_options)

    config['title'] = 'Daily C* regression suite - {}'.format(datetime.datetime.now().strftime("%Y-%m-%d"))

    if title is not None:
        config['title'] += ' - {title}'.format(title=title)

    return config


def compressible_profile(title='Compressible', cluster='blade_11', n='100M', threads=300, yaml=None):
    # use belliottsmith's stress-compressible branch for the stress executable
    config = create_baseline_config(title,
                                    revision_config_options={'stress_revision': 'belliottsmith/stress-compressible'})
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': 'user profile=https://raw.githubusercontent.com/mambocab/cstar_perf/belliottsmith-perf-jobs/regression_suites/compressible.yaml '
                    'ops\(insert=100,latest=100,point=10,range=1\) '
                    'n={n} -rate threads={threads} -pop seq=1..10K '
                    'read-lookback=uniform\(1..1M\) -insert visits=fixed\(10K\) '
                    'revisit=uniform\(1..10K\)'.format(n=n,
                                                       threads=threads)},
    ]
    if yaml:
        config['yaml'] = yaml

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def test_compressible_profile():
    compressible_profile()


def trades_with_flags_profile(title='Trades With Flags', cluster='blade_11', n='100M', threads=300, yaml=None):
    config = create_baseline_config(title)
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': 'user profile=https://raw.githubusercontent.com/mambocab/cstar_perf/belliottsmith-perf-jobs/regression_suites/trades-with-flags.yaml '
                    'ops\(insert=100,latest=100,point=10,range=1\) '
                    'n={n} -rate threads={threads} -pop seq=1..10K contents=SORTED '
                    'read-lookback=uniform\(1..1M\) -insert visits=fixed\(10K\) '
                    'revisit=uniform\(1..10K\)'.format(n=n,
                                                       threads=threads)}
    ]
    if yaml:
        config['yaml'] = yaml

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def test_trades_with_flags_profile():
    trades_with_flags_profile()


def trades_profile(title='Trades', cluster='blade_11', n='100M', threads=300, yaml=None):
    config = create_baseline_config(title)
    config['cluster'] = cluster
    config['operations'] = [
        {'operation': 'stress',
         'command': 'user profile=https://raw.githubusercontent.com/mambocab/cstar_perf/belliottsmith-perf-jobs/regression_suites/trades.yaml '
                    'ops\(insert=100,latest=100,point=10,range=1\) '
                    'n={n} -rate threads={threads} -pop seq=1..10K contents=SORTED '
                    'read-lookback=uniform\(1..1M\) -insert visits=fixed\(10K\) '
                    'revisit=uniform\(1..10K\)'.format(n=n,
                                                       threads=threads)}
    ]
    if yaml:
        config['yaml'] = yaml

    scheduler = Scheduler(CSTAR_SERVER)
    scheduler.schedule(config)


def test_trades_profile():
    trades_profile()
