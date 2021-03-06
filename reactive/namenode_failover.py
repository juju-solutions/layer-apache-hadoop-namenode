from operator import itemgetter
from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import when_any
from charms.reactive import is_state
from charms.reactive import set_state
from charms.reactive import remove_state
from charms.reactive.helpers import data_changed
from charms.layer.hadoop_base import get_hadoop_base
from charms.layer.apache_hadoop_namenode import get_cluster_nodes
from charmhelpers.core import hookenv
from jujubigdata.handlers import HDFS
from charms import leadership


@when('zookeeper.ready')
@when('leadership.set.ha-initialized')
def update_zk_config(zookeeper):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    zk_nodes = sorted(zookeeper.zookeepers(), key=itemgetter('host'))
    zk_started = is_state('namenode.zk.started')
    hdfs.configure_zookeeper(zk_nodes)
    if zk_started and data_changed('namenode.zk', zk_nodes):
        hdfs.restart_zookeeper()


@when('zookeeper.ready')
@when('leadership.set.ha-initialized')
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
@when_any('leadership.set.cluster-nodes', 'leadership.changed.cluster-nodes')
@when('namenode.started')
@when_any('namenode-cluster.standby.ready', 'namenode.standby')
@when_not('namenode.zk.started')
def start_zookeeper(zookeeper):
    local_hostname = hookenv.local_unit().replace('/', '-')
    if local_hostname not in get_cluster_nodes():
        # can't run zkfc on a non-cluster node
        return
    update_zk_config(zookeeper)  # ensure config is up to date
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.restart_namenode()
    hdfs.start_zookeeper()
    set_state('namenode.zk.started')


@when_not('zookeeper.ready')
@when('namenode.zk.started')
def stop_zookeeper():
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.stop_zookeeper()
    remove_state('namenode.zk.started')
