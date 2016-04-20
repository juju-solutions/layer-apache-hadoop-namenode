from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import is_state
from charms.reactive import set_state
from charms.reactive import remove_state
from charms.reactive import toggle_state
from charms.reactive.helpers import data_changed
from charms.layer.hadoop_base import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv
from charms import leadership
from charms.layer.apache_hadoop_namenode import get_cluster_nodes
from charms.layer.apache_hadoop_namenode import set_cluster_nodes


@when('namenode-cluster.joined')
def manage_cluster_hosts(cluster):
    utils.update_kv_hosts(cluster.hosts_map())
    utils.manage_etc_hosts()


@when('datanode.journalnode.joined')
def check_journalnode_quorum(datanode):
    nodes = datanode.nodes()
    quorum_size = hookenv.config('journalnode_quorum_size')
    toggle_state('journalnode.quorum', len(nodes) >= quorum_size)


@when('namenode-cluster.joined', 'journalnode.quorum')
@when('leadership.is_leader')
def enable_ha(cluster):
    """
    Once we have two viable NameNodes and a quorum of JournalNodes,
    inform all cluster units that we are HA.

    Note that this flag is never removed (once HA, always HA) because we
    could temporarily lose JN quorum or viable NNs during fail-over or
    restart, and we don't want to arbitrarily revert to non-HA in that case.
    """
    leadership.leader_set({'namenode-ha': 'true'})


@when('leadership.set.ha-initialized')
@when('namenode-cluster.joined')
def check_cluster_health(cluster):
    cluster_roles = set(utils.ha_node_state(node)
                        for node in get_cluster_nodes())
    toggle_state('namenode.cluster.healthy',
                 cluster_roles == {'active', 'standby'})


@when('leadership.set.cluster-activated')
@when('namenode-cluster.joined')
@when('leadership.is_leader')
def check_cluster_nodes(cluster):
    """
    Check to see if any of the chosen cluster nodes have gone away and
    been replaced by viable replacements.

    Note that we only remove a chosen node if it is no longer part of
    the peer relation *and* has been replaced by a working node.  This
    ensures that reboots and intermittent node loses don't cause
    superfluous updates.
    """
    local_hostname = hookenv.local_unit().replace('/', '-')
    hadoop = get_hadoop_base()
    hdfs_port = hadoop.dist_config.port('namenode')

    manage_cluster_hosts(cluster)  # ensure /etc/hosts is up-to-date

    chosen_nodes = set(get_cluster_nodes())
    current_nodes = set([local_hostname] + cluster.nodes())
    remaining_nodes = chosen_nodes & current_nodes
    added_nodes = current_nodes - chosen_nodes

    if len(remaining_nodes) < 2 and added_nodes:
        # filter down to only responding nodes
        viable_nodes = [node for node in added_nodes
                        if utils.check_connect(node, hdfs_port)]
        if viable_nodes:
            set_cluster_nodes(sorted(remaining_nodes) + sorted(viable_nodes))

    check_cluster_health(cluster)  # update health check


@when('leadership.set.ha-initialized', 'datanode.joined')
def update_ha_config(datanode):
    cluster_nodes = get_cluster_nodes()
    jn_nodes = sorted(datanode.nodes())
    jn_port = datanode.jn_port()
    started = is_state('namenode.started')
    new_cluster_config = data_changed('namenode.cluster-nodes', cluster_nodes)
    new_jn_config = data_changed('namenode.jn.config', (jn_nodes, jn_port))

    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_namenode(cluster_nodes)
    hdfs.register_journalnodes(jn_nodes, jn_port)

    if started and new_cluster_config:
        hdfs.restart_namenode()
    elif started and new_jn_config:
        hdfs.reload_slaves()  # is this actually necessary?


@when('leadership.set.namenode-ha')
@when('namenode-cluster.joined')
@when('datanode.journalnode.joined')
@when_not('leadership.set.ha-initialized')
@when('leadership.is_leader')
def init_ha_active(datanode, cluster):
    """
    Do initial HA setup on the leader.
    """
    local_hostname = hookenv.local_unit().replace('/', '-')
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.stop_namenode()
    remove_state('namenode.started')
    # initial cluster is us (active) plus a standby
    set_cluster_nodes([local_hostname, cluster.nodes()[0]])
    update_ha_config(datanode)
    hdfs.init_sharededits()
    hdfs.start_namenode()
    leadership.leader_set({'ha-initialized': 'true'})
    set_state('namenode.started')


@when('namenode-cluster.joined')
@when('datanode.journalnode.joined')
@when('leadership.set.ha-initialized')  # wait for leader to init HA
@when_not('namenode.started')
def init_ha_standby(datanode, cluster):
    """
    Once initial HA setup is done, any new NameNode is started as standby.
    """
    update_ha_config(datanode)  # ensure the config is written
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    update_ha_config(datanode)
    hdfs.bootstrap_standby()
    hdfs.start_namenode()
    cluster.standby_ready()
    set_state('namenode.standby')
    hadoop.open_ports('namenode')
    set_state('namenode.started')


# FIXME: The transition to active fails; uncertain if it would fail if done
#        manually by the admin as well, or if it's just a timing issue; needs
#        more testing.  It would be nice, though, if the transition to HA would
#        end up with an active node without manual intervention, even without
#        Zookeeper in the mix.  But it's not the end of the world if not.
#
# @when('leadership.set.ha-initialized')
# @when('namenode-cluster.standby.ready')
# @when_not('leadership.set.cluster-activated')
# @when('leadership.is_leader')
# def activate_cluster(cluster):
#     """
#     Once both the active and standby NameNodes have been started, we
#     can transition the leader to active.  This must only be done once.
#     """
#     chosen_standby = get_cluster_nodes()[1]
#     if chosen_standby not in cluster.nodes():
#         return
#     local_hostname = hookenv.local_unit().replace('/', '-')
#     hadoop = get_hadoop_base()
#     hdfs = HDFS(hadoop)
#     hdfs.transition_to_active(local_hostname)
#     leadership.leader_set({'cluster-activated': 'true'})
