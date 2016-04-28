#!/usr/bin/env python3
from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import when_any
from charms.reactive import is_state
from charms.reactive import RelationBase
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
    cluster_roles = set(utils.ha_node_state(node, 1) for node in chosen_nodes)
    chosen = local_hostname in chosen_nodes
    started = is_state('namenode.started')
    ha = is_state('leadership.set.namenode-ha')
    clustered = is_state('namenode-cluster.joined')
    active = 'active' in cluster_roles
    standby = 'standby' in cluster_roles
    healthy = active and standby
    quorum = is_state('journalnode.quorum')
    failover = 'automatic' if is_state('zookeeper.ready') else 'manual'
    degraded = ha and not all([clustered, quorum, healthy])

    if not ha:
        if started:
            extra = 'standalone'
        else:
            extra = 'down'
    else:
        if chosen:
            role = utils.ha_node_state(local_hostname) or 'down'
        else:
            role = 'extra'
        if not degraded:
            extra = 'HA {}, with {} fail-over'.format(role, failover)
        else:
            missing = ' and '.join(filter(None, [
                'NameNode' if not clustered else None,
                'JournalNodes' if not quorum else None,
                'active' if not active else None,
                'standby' if not standby else None,
            ]))
            extra = 'HA degraded {} (missing: {}), with {} fail-over'.format(
                role, missing, failover)
    hookenv.status_set('active', 'Ready ({count} DataNode{s}, {extra})'.format(
        count=num_slaves,
        s='s' if num_slaves > 1 else '',
        extra=extra,
    ))


if __name__ == '__main__':
    if is_state('datanode.joined'):
        report_status(RelationBase.from_state('datanode.joined'))
    else:
        report_blocked()
