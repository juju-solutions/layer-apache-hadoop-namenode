from operator import itemgetter
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


@when('namenode-cluster.joined')
@when_not('leadership.is_leader')
def check_cluster_nodes(cluster):
    hadoop = get_hadoop_base()
    hdfs_port = hadoop.dist_config.port('namenode')

    manage_cluster_hosts(cluster)  # ensure /etc/hosts is up-to-date

    chosen_nodes = get_cluster_nodes()
    extra_nodes = sorted(set(cluster.nodes()) - set(chosen_nodes))

    if is_state('namenode.ha.initialized'):
        # filter down to only responding nodes, preferring
        # nodes we've already chosen over new nodes
        viable_nodes = [node for node in chosen_nodes + extra_nodes
                        if utils.check_connect(node, hdfs_port)]
    else:
        # if HA is not yet initialized, then the other node won't
        # yet be running, so just assume all nodes are viable
        viable_nodes = chosen_nodes + extra_nodes

    cluster_viable = len(viable_nodes) >= 2
    hookenv.log('Viable nodes: {}'.format(viable_nodes))
    toggle_state('namenode.clustered', cluster_viable)
    return viable_nodes


@when('namenode-cluster.joined')
@when('leadership.is_leader')
def choose_cluster_nodes(cluster):
    viable_nodes = check_cluster_nodes(cluster)
    cluster_viable = len(viable_nodes) >= 2
    if cluster_viable:
        # only update chosen nodes if we (again) have enough viable nodes
        set_cluster_nodes(viable_nodes[:2])


@when('datanode.journalnode.joined')
def check_journalnode_quorum(datanode):
    nodes = datanode.nodes()
    quorum_size = hookenv.config('journalnode_quorum_size')
    toggle_state('journalnode.quorum', len(nodes) >= quorum_size)


@when_not('datanode.journalnode.joined')
def remove_journalnode_quorum():
    remove_state('journalnode.quorum')


@when('namenode.clustered', 'journalnode.quorum')
def enable_ha():
    set_state('namenode.ha')  # once HA, always HA


@when('namenode.ha', 'datanode.joined')
def update_ha_config(datanode):
    cluster_nodes = get_cluster_nodes()
    jn_nodes = sorted(datanode.nodes())
    jn_port = datanode.jn_port()
    started = is_state('namenode.started')
    ha_inited = is_state('namenode.ha.initialized')
    new_cluster_config = data_changed('namenode.cluster-nodes', cluster_nodes)
    new_jn_config = data_changed('namenode.jn.config', (jn_nodes, jn_port))

    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_namenode(cluster_nodes)
    hdfs.register_journalnodes(jn_nodes, jn_port)

    if started and ha_inited and new_cluster_config:
        hdfs.restart_namenode()
    elif started and ha_inited and new_jn_config:
        hdfs.reload_slaves()


@when('namenode.ha', 'datanode.joined')
@when_not('leadership.set.ha-initialized')
@when('leadership.is_leader')
def init_ha_active(datanode):
    local_hostname = hookenv.local_unit().replace('/', '-')
    update_ha_config(datanode)  # ensure the config is written
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.stop_namenode()
    remove_state('namenode.started')
    update_ha_config(datanode)
    hdfs.init_sharededits()
    hdfs.start_namenode()
    hdfs.transition_to_active(local_hostname)
    leadership.leader_set({'ha-initialized': 'true'})
    set_state('namenode.ha.initialized')
    set_state('namenode.started')


@when('namenode.ha', 'datanode.joined')
@when('leadership.set.ha-initialized')
@when_not('leadership.is_leader')
@when_not('namenode.ha.initialized')
def init_ha_standby(datanode):
    update_ha_config(datanode)  # ensure the config is written
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.stop_namenode()
    remove_state('namenode.started')
    update_ha_config(datanode)
    hdfs.bootstrap_standby()
    hdfs.start_namenode()
    set_state('namenode.ha.initialized')
    set_state('namenode.started')


@when('zookeeper.ready')
@when('namenode.ha.initialized')
def update_zk_config(zookeeper):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    zk_nodes = sorted(zookeeper.zookeepers(), key=itemgetter('host'))
    zk_started = is_state('namenode.zk.started')
    hdfs.configure_zookeeper(zk_nodes)
    if zk_started and data_changed('namenode.zk', zk_nodes):
        hdfs.restart_zookeeper()


@when('namenode.ha.initialized')
@when('zookeeper.ready')
@when_not('leadership.set.zk-formatted')
@when('leadership.is_leader')
def format_zookeeper(zookeeper):
    update_zk_config(zookeeper)  # ensure config is up to date
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.format_zookeeper()
    leadership.leader_set({'zk-formatted': 'true'})


@when('zookeeper.ready')
@when('leadership.set.zk-formatted')
@when_not('namenode.zk.started')
def start_zookeeper(zookeeper):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.start_zookeeper()
    set_state('namenode.zk.started')


@when_not('zookeeper.ready')
@when('namenode.zk.started')
def stop_zookeeper():
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.stop_zookeeper()
    remove_state('namenode.zk.started')
