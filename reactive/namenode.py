import time
from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.reactive.bus import get_state
from charms.reactive import remove_state
from charms.reactive import is_state
from charms.reactive.helpers import data_changed
from charms.layer.hadoop_base import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv, unitdata


@when('hadoop.installed')
@when_not('namenode.started')
def configure_namenode():
    local_hostname = hookenv.local_unit().replace('/', '-')
    private_address = hookenv.unit_get('private-address')
    ip_addr = utils.resolve_private_address(private_address)
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_namenode([local_hostname])
    hdfs.format_namenode()
    hdfs.start_namenode()
    if hookenv.is_leader():
        try:
            hookenv.leader_get('hdfs_initalized')
        except NameError:
            hookenv.leader_set(hdfs_initialized='False')
        if hookenv.leader_get('hdfs_initialized') == 'False':
            hdfs.create_hdfs_dirs()
            hookenv.leader_set(hdfs_initialized='True')
    hadoop.open_ports('namenode')
    utils.update_kv_hosts({ip_addr: local_hostname})
    set_state('namenode.started')


@when('namenode.started')
@when_not('datanode.joined')
def blocked():
    hookenv.status_set('blocked', 'Waiting for relation to DataNodes')


# if you run this degraded thing too early, then /etc/hosts doesn't contain the other namenode..!
#@when('namenode-cluster.joined')
#def hdfs_HA_degraded(cluster, *args):
#    hadoop = get_hadoop_base()
#    hdfs_port = hadoop.dist_config.port('namenode')
#    if not cluster.check_peer_port(hdfs_port):
#        set_state('hdfs.degraded')
#    else:
#        remove_state('hdfs.degraded')


class TimeoutError(Exception):
    pass

@when('namenode.started', 'datanode.related', 'namenode-cluster.initialized')
def send_info_ha(datanode, cluster):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    cluster_nodes = cluster.nodes()
    hdfs_port = hadoop.dist_config.port('namenode')
    webhdfs_port = hadoop.dist_config.port('nn_webapp_http')

    utils.update_kv_hosts(datanode.hosts_map())
    utils.manage_etc_hosts()

    datanode.send_spec(hadoop.spec())
    datanode.send_clustername(hookenv.service_name())
    datanode.send_namenodes(cluster_nodes)
    datanode.send_ports(hdfs_port, webhdfs_port)
    datanode.send_ssh_key(utils.get_ssh_key('hdfs'))
    datanode.send_hosts_map(utils.get_kv_hosts())

    slaves = datanode.nodes()
    if data_changed('namenode.slaves', slaves):
        hookenv.log("Waiting for other namenode...")
        start = time.time()
        while time.time() - start < 30:
            if cluster.check_peer_port(hdfs_port):
                remove_state('hdfs.degraded')
                unitdata.kv().set('namenode.slaves', slaves)
                hdfs.register_slaves(slaves)
                hdfs.reload_slaves()
                return True
            else:
                hookenv.status_set('waiting', 'HDFS HA degraded - waiting for peer...')
                set_state('hdfs.degraded')
            time.sleep(2)
        raise TimeoutError('Timed out waiting for other namenode')
        
    hookenv.status_set('active', 'Ready ({count} DataNode{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))
    set_state('namenode.ready')


@when('namenode.started', 'datanode.related')
@when_not('namenode-cluster.initialized')
def send_info(datanode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    local_hostname = hookenv.local_unit().replace('/', '-')
    hdfs_port = hadoop.dist_config.port('namenode')
    webhdfs_port = hadoop.dist_config.port('nn_webapp_http')

    utils.update_kv_hosts(datanode.hosts_map())
    utils.manage_etc_hosts()

    datanode.send_spec(hadoop.spec())
    datanode.send_clustername(hookenv.service_name())
    datanode.send_namenodes([local_hostname])
    datanode.send_ports(hdfs_port, webhdfs_port)
    datanode.send_ssh_key(utils.get_ssh_key('hdfs'))
    datanode.send_hosts_map(utils.get_kv_hosts())

    slaves = datanode.nodes()
    if data_changed('namenode.slaves', slaves):
        unitdata.kv().set('namenode.slaves', slaves)
        hdfs.register_slaves(slaves)
        hdfs.reload_slaves()

    hookenv.status_set('active', 'Ready ({count} DataNode{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))
    set_state('namenode.ready')


#@when('namenode-cluster.joined')
#@when_not('leader.elected')
#def wait_for_leader(*args):
#    try:
#        hookenv.leader_get('leader_elected')
#        set_state('leader.elected')
#    except NameError:
#        hookenv.status_set('waiting', 'Waiting for Leader to be elected...')
#        if hookenv.is_leader():
#            hookenv.leader_set(leader_elected='True')


@when('namenode-cluster.joined', 'datanode.journalnode.ha')  # , 'leader.elected')
def configure_ha(cluster, datanode, *args):
    set_state('hdfs.ha.initialized')
    cluster_nodes = cluster.nodes()
    jn_nodes = datanode.nodes()
    jn_port = datanode.jn_port()
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    cluster_nodes = cluster.nodes()
    if datanode.journalnodes_ready():
        if data_changed('namenode.ha', cluster_nodes):
            utils.update_kv_hosts(cluster.hosts_map())
            utils.manage_etc_hosts()
            datanode.send_namenodes(cluster_nodes)
            hdfs.configure_namenode(cluster_nodes)
            hdfs.restart_namenode()
        if data_changed('namenode.ha', [jn_nodes, jn_port]):
            utils.manage_etc_hosts()
            hdfs.register_journalnodes(jn_nodes, jn_port)
        if hookenv.is_leader():
            if not is_state('namenode.shared-edits.init'):
                hdfs.stop_namenode()
                hdfs.init_sharededits()
                set_state('namenode.shared-edits.init')
                cluster.jns_init()
                remove_state('hdfs.degraded')
                local_hostname = hookenv.local_unit().replace('/', '-')
                hdfs.start_namenode()
                hdfs.ensure_HA_active(cluster_nodes, local_hostname)
                # 'leader' appears to transition back to standby after restart - test more
        elif not hookenv.is_leader():
            if not is_state('namenode.standby.bootstrapped') and cluster.are_jns_init():
                hdfs.stop_namenode()
                hdfs.format_namenode()
                remove_state('hdfs.degraded')
                #hdfs.configure_namenode(cluster_nodes)
                hdfs.bootstrap_standby()
                set_state('namenode.standby.bootstrapped')
                hdfs.start_namenode()
            else:
                hookenv.status_set('waiting', 'Waiting for 3 slaves to initialize HDFS HA')


#if hookenv.is_leader():
#    @when('namenode-cluster.joined', 'datanode.journalnode.ha')
#    @when_not('zookeeper.joined')
#    def ensure_active(cluster, datanode):
#        '''
#        If we enable HA before zookeeper is connected, we need to at least
#        ensure that one namenode is active and one is standby to ensure that
#        hdfs is functional
#        '''
#        hadoop = get_hadoop_base()
#        hdfs = HDFS(hadoop)
#        local_hostname = hookenv.local_unit().replace('/', '-')
#        hookenv.log('Zookeeper not related, failovers will not be automatic')
#        cluster_nodes = cluster.nodes()
#        hdfs.ensure_HA_active(cluster_nodes, local_hostname)


@when('namenode.clients')
@when('namenode.ready')
def accept_clients(clients):
    hadoop = get_hadoop_base()
    local_hostname = hookenv.local_unit().replace('/', '-')
    hdfs_port = hadoop.dist_config.port('namenode')
    webhdfs_port = hadoop.dist_config.port('nn_webapp_http')

    clients.send_spec(hadoop.spec())
    # How to handle send_namenodes here?
    clients.send_namenodes([local_hostname])
    clients.send_ports(hdfs_port, webhdfs_port)
    clients.send_hosts_map(utils.get_kv_hosts())
    clients.send_ready(True)


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

    utils.remove_kv_hosts(slaves_leaving)
    utils.manage_etc_hosts()

    if not slaves_remaining:
        hookenv.status_set('blocked', 'Waiting for relation to DataNodes')
        remove_state('namenode.ready')

    datanode.dismiss()


@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('nnbench', 'testdfsio')
