from charms.reactive import when, when_not, set_state, is_state
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
    hookenv.status_set('blocked', 'Waiting for DataNodes')
    set_state('namenode.started')


@when('namenode.started', 'hdfs.client.connected')
def configure_client(hdfs_rel):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs_port = hdfs.dist_config.port('namenode')
    webhdfs_port = hdfs.dist_config.port('nn_webapp_http')

    hdfs_rel.send_spec(hdfs.spec)
    hdfs_rel.send_ports(hdfs_port, webhdfs_port)
    hdfs_rel.send_host_map(utils.get_kv_hosts())
    hdfs_rel.send_ready(is_state('namenode.ready'))


@when('namenode.started', 'hdfs.datanode.connected')
def configure_datanodes(hdfs_rel):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    datanodes = hdfs_rel.datanodes()
    slaves = [node['hostname'] for node in datanodes]
    hdfs.register_slaves(slaves)
    for node in datanodes:
        utils.update_kv_host(node['ip'], node['hostname'])
    hdfs_rel.send_ssh_key(utils.get_ssh_key('ubuntu'))
    hookenv.status_set('active', 'Ready ({} DataNodes)'.format(len(datanodes)))
    set_state('namenode.ready')


@when('namenode.started', 'hdfs.secondary.connected')
def configure_secondary(hdfs_rel):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    secondary = hdfs_rel.secondaries()[0]  # there can be only one
    hdfs.configure_namenode(secondary['hostname'], secondary['port'])
    utils.update_kv_host(secondary['ip'], secondary['hostname'])
    hdfs_rel.send_ssh_key(utils.get_ssh_key('ubuntu'))
