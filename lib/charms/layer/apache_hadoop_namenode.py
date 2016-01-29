import json
from charms import leadership


def get_cluster_nodes():
    return json.loads(leadership.leader_get('cluster-nodes') or '[]')


def set_cluster_nodes(nodes):
    leadership.leader_set({
        'cluster-nodes': json.dumps(sorted(nodes)),
    })
