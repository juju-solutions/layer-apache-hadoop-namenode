from charms.reactive import when, when_not, set_state, remove_state
from charms.hadoop import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv


@when('hadoop.installed')
@when_not('namenode.started')
def configure_namenode():
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_namenode()
    hdfs.format_namenode()
    hdfs.start_namenode()
    hdfs.create_hdfs_dirs()
    hadoop.open_ports('namenode')
    set_state('namenode.started')


@when('namenode.started')
@when_not('hdfs.datanode.connected')
def status_blocked():
    hookenv.status_set('blocked', 'Waiting for DataNodes')
    remove_state('namenode.ready')


@when('hdfs.client.connected')
@when_not('namenode.ready')
def send_blocked(client):
    client.send_ready(False)


@when('hdfs.client.connected')
@when('namenode.ready')
def send_ready(client):
    client.send_ready(True)


@when('namenode.started', 'hdfs.client.connected')
def configure_client(hdfs_rel):
    hadoop = get_hadoop_base()
    hdfs_port = hadoop.dist_config.port('namenode')
    webhdfs_port = hadoop.dist_config.port('nn_webapp_http')

    hdfs_rel.send_spec(hadoop.spec())
    hdfs_rel.send_ports(hdfs_port, webhdfs_port)
    hdfs_rel.send_hosts_map(utils.get_kv_hosts())


@when('namenode.started', 'hdfs.datanode.connected')
def configure_datanodes(hdfs_rel):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    datanodes = hdfs_rel.datanodes()
    slaves = [node['hostname'] for node in datanodes]
    hdfs.register_slaves(slaves)
    utils.update_kv_hosts({node['ip']: node['hostname'] for node in datanodes})
    utils.manage_etc_hosts()
    hdfs_rel.send_ssh_key(utils.get_ssh_key('ubuntu'))
    hookenv.status_set('active', 'Ready ({count} DataNode{s})'.format(
        count=len(datanodes),
        s='s' if len(datanodes) > 1 else '',
    ))
    set_state('namenode.ready')


@when('namenode.started', 'hdfs.secondary.connected')
def configure_secondary(hdfs_rel):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    secondary = hdfs_rel.secondaries()[0]  # there can be only one
    hdfs.configure_namenode(secondary['hostname'], secondary['port'])
    utils.update_kv_host(secondary['ip'], secondary['hostname'])
    utils.manage_etc_hosts()
    hdfs_rel.send_ssh_key(utils.get_ssh_key('ubuntu'))
