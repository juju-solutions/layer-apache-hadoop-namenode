from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import when_any
from charms.reactive import is_state
from charmhelpers.core import hookenv
from jujubigdata import utils
from charms.layer.apache_hadoop_namenode import get_cluster_nodes


@when_not('datanode.joined')
def report_blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Slave')


@when('datanode.joined')
@when_any(
    'namenode.started',
    'leadership.set.namenode-ha',
    'namenode-cluster.joined',
    'namenode.cluster.healthy',
    'journalnode.quorum',
    'zookeeper.ready',
)
def report_status(datanode):
    num_slaves = len(datanode.nodes())
    local_hostname = hookenv.local_unit().replace('/', '-')
    chosen_nodes = get_cluster_nodes()
    chosen = local_hostname in chosen_nodes
    started = is_state('namenode.started')
    ha = is_state('leadership.set.namenode-ha')
    pending_ha = not ha and is_state('namenode-cluster.joined')
    standalone = started and not pending_ha and not ha
    clustered = is_state('namenode.cluster.healthy')
    quorum = is_state('journalnode.quorum')
    failover = 'automatic' if is_state('zookeeper.ready') else 'manual'
    degraded = ha and not all([clustered, quorum])
    if started:
        pending_role = 'active'
    elif chosen:
        pending_role = 'standby'
    else:
        pending_role = 'standby or extra'
    if chosen:
        ha_role = utils.ha_node_state(local_hostname) or 'down'
    else:
        ha_role = 'extra'

    if standalone:
        extra = 'standalone'
    elif ha and not degraded:
        extra = '{}, with {} fail-over'.format(ha_role, failover)
    elif (ha and degraded) or pending_ha:
        if pending_ha:
            clustered = is_state('namenode-cluster.joined')
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
