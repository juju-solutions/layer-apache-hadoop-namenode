from subprocess import check_output, CalledProcessError
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
@when_not('leadership.set.namenode-ha')
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
@when('datanode.joined')
@when('namenode-cluster.joined')
@when('leadership.is_leader')
def check_cluster_nodes(cluster, datanode):
    """
    Check to see if any of the chosen cluster nodes have gone away and
    been replaced by viable replacements.

    Note that we only remove a chosen node if it is no longer part of
    the peer relation *and* has been replaced by a working node.  This
    ensures that reboots and intermittent node loses don't cause
    superfluous updates.
    """
    local_hostname = hookenv.local_unit().replace('/', '-')

    manage_cluster_hosts(cluster)  # ensure /etc/hosts is up-to-date

    chosen_nodes = set(get_cluster_nodes())
    current_nodes = set([local_hostname] + cluster.nodes())
    remaining_nodes = chosen_nodes & current_nodes
    added_nodes = current_nodes - chosen_nodes

    if len(remaining_nodes) < 2 and added_nodes:
        chosen_nodes = (sorted(remaining_nodes) + sorted(added_nodes))[:2]
        set_cluster_nodes(chosen_nodes)
        update_ha_config(datanode)  # ensure new config gets written


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
    local_hostname = hookenv.local_unit().replace('/', '-')
    if local_hostname not in get_cluster_nodes():
        # can't even bootstrapStandby if not in the list of chosen nodes
        return
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


@when('leadership.set.ha-initialized')
@when('namenode.started')
@when_not('namenode.crontab')
def register_crontab():
    local_unit = hookenv.local_unit()
    try:
        crontab = check_output(['crontab', '-lu', 'hdfs'],
                               universal_newlines=True)
    except CalledProcessError:
        crontab = ''
    crontab += '*/1 * * * * ' \
               'juju-run %s "PYTHONPATH=lib reactive/namenode_status.py"\n' % (
                   local_unit)
    check_output(['crontab', '-', '-u', 'hdfs'],
                 input=crontab, universal_newlines=True)
    set_state('namenode.crontab')
