from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import when_any
from charms.reactive import is_state
from charmhelpers.core import hookenv
from jujubigdata import utils


@when_not('datanode.joined')
def report_blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Slave')


@when('datanode.joined')
@when_any(
    'namenode.started',
    'namenode.ha',
    'namenode-cluster.joined',
    'namenode.clustered',
    'journalnode.quorum',
    'zookeeper.ready',
)
def report_status(datanode):
    num_slaves = len(datanode.nodes())
    local_hostname = hookenv.local_unit().replace('/', '-')
    started = is_state('namenode.started')
    ha = is_state('namenode.ha')
    ha_init = is_state('namenode.ha.initialized')
    pending_ha = not ha and is_state('namenode-cluster.joined')
    pending_role = 'active' if started else 'standby'
    standalone = started and not pending_ha and not ha
    clustered = is_state('namenode.clustered')
    quorum = is_state('journalnode.quorum')
    failover = 'automatic' if is_state('zookeeper.ready') else 'manual'
    degraded = ha and not all([clustered, quorum])
    ha_role = utils.ha_node_state(local_hostname) if ha_init else 'unknown'

    if standalone:
        extra = 'standalone'
    elif ha and not degraded:
        extra = '{}, with {} fail-over'.format(ha_role, failover)
    elif (ha and degraded) or pending_ha:
        condition = 'pending' if pending_ha else 'degraded'
        role = pending_role if pending_ha else ha_role
        missing = ' and '.join(filter(None, [
            'NameNode' if not clustered else None,
            'JournalNodes' if not quorum else None,
        ]))
        extra = '{} {} (missing: {}), with {} fail-over'.format(
            condition, role, missing, failover)
    else:
        extra = 'unknown'
    hookenv.status_set('active', 'Ready ({count} DataNode{s}, {extra})'.format(
        count=num_slaves,
        s='s' if num_slaves > 1 else '',
        extra=extra,
    ))
