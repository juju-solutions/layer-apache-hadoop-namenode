from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.reactive import remove_state
from charms.reactive.helpers import data_changed
from charms.layer.hadoop_base import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv, unitdata
from charms import leadership
from charms.layer.apache_hadoop_namenode import get_cluster_nodes
from charms.layer.apache_hadoop_namenode import set_cluster_nodes


@when('hadoop.installed')
@when_not('namenode.started')
@when('leadership.is_leader')  # don't both starting standalone if not leader
@when('leadership.set.cluster-nodes')
def configure_namenode():
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_namenode(get_cluster_nodes())
    hdfs.format_namenode()
    hdfs.start_namenode()
    hdfs.create_hdfs_dirs()
    hadoop.open_ports('namenode')
    utils.initialize_kv_host()
    utils.manage_etc_hosts()
    set_state('namenode.started')


@when('hadoop.installed', 'leadership.is_leader')
@when_not('leadership.set.ssh-key-pub')
def generate_ssh_key():
    utils.generate_ssh_key('hdfs')
    leadership.leader_set({
        'ssh-key-priv': utils.ssh_priv_key('hdfs').text(),
        'ssh-key-pub': utils.ssh_pub_key('hdfs').text(),
    })


@when('leadership.changed.ssh-key-pub')
def install_ssh_key():
    ssh_dir = utils.ssh_key_dir('hdfs')
    ssh_dir.makedirs_p()
    authfile = ssh_dir / 'authorized_keys'
    authfile.write_lines(leadership.leader_get('ssh-key-pub'), append=True)


@when('datanode.joined')
def manage_datanode_hosts(datanode):
    utils.update_kv_hosts(datanode.hosts_map())
    utils.manage_etc_hosts()
    datanode.send_hosts_map(utils.get_kv_hosts())


@when('datanode.joined', 'leadership.set.ssh-key-pub')
def send_ssh_key(datanode):
    datanode.send_ssh_key(leadership.leader_get('ssh-key-pub'))


@when('leadership.is_leader')
@when_not('leadership.set.cluster-nodes')
def init_cluster_nodes():
    local_hostname = hookenv.local_unit().replace('/', '-')
    set_cluster_nodes([local_hostname])


@when('namenode.started', 'datanode.joined')
def send_info(datanode):
    hadoop = get_hadoop_base()
    hdfs_port = hadoop.dist_config.port('namenode')
    webhdfs_port = hadoop.dist_config.port('nn_webapp_http')

    datanode.send_spec(hadoop.spec())
    datanode.send_clustername(hookenv.service_name())
    datanode.send_namenodes(get_cluster_nodes())
    datanode.send_ports(hdfs_port, webhdfs_port)


@when('namenode.started', 'datanode.joined')
def update_slaves(datanode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    slaves = datanode.nodes()
    if data_changed('namenode.slaves', slaves):
        unitdata.kv().set('namenode.slaves', slaves)
        hdfs.register_slaves(slaves)
        hdfs.reload_slaves()

    set_state('namenode.ready')


@when('namenode.started', 'datanode.joined')
@when('leadership.changed.cluster-nodes')
def update_nodes(datanode):
    datanode.send_namenodes(get_cluster_nodes())


@when('namenode.ready')
@when('namenode.clients')
def accept_clients(clients):
    hadoop = get_hadoop_base()
    hdfs_port = hadoop.dist_config.port('namenode')
    webhdfs_port = hadoop.dist_config.port('nn_webapp_http')

    clients.send_spec(hadoop.spec())
    clients.send_clustername(hookenv.service_name())
    clients.send_namenodes(get_cluster_nodes())
    clients.send_ports(hdfs_port, webhdfs_port)
    clients.send_hosts_map(utils.get_kv_hosts())
    clients.send_ready(True)


@when('namenode.ready')
@when('namenode.clients')
@when('leadership.changed.cluster-nodes')
def update_clients(clients):
    clients.send_namenodes(get_cluster_nodes())


@when('namenode.clients')
@when_not('namenode.ready')
def reject_clients(clients):
    clients.send_ready(False)


@when('namenode.started', 'datanode.departing')
def unregister_datanode(datanode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)

    slaves = unitdata.kv().get('namenode.slaves', [])
    slaves_leaving = datanode.nodes()  # only returns nodes in "leaving" state
    hookenv.log('Slaves leaving: {}'.format(slaves_leaving))

    slaves_remaining = list(set(slaves) - set(slaves_leaving))
    unitdata.kv().set('namenode.slaves', slaves_remaining)
    hdfs.register_slaves(slaves_remaining)
    hdfs.reload_slaves()

    utils.remove_kv_hosts(slaves_leaving)
    utils.manage_etc_hosts()

    if not slaves_remaining:
        remove_state('namenode.ready')

    datanode.dismiss()


@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('nnbench', 'testdfsio')
