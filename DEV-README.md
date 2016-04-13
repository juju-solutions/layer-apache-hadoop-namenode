## Overview

This charm provides storage resource management for an Apache Hadoop
deployment, and is intended to be used only as a part of that deployment.
This document describes how this charm connects to and interacts with the
other components of the deployment.

Note: This charm is build from a [charm layer][] and a [base layer][].
Additionally, most of the implementation is handled in a [library][].
Changes should be made in the appropriate place and then this charm
should be rebuilt using [charm build][].

[charm layer]: https://github.com/juju-solutions/apache-hadoop-namenode
[base layer]: https://github.com/juju-solutions/layer-hadoop-base
[library]: https://github.com/juju-solutions/jujubigdata
[charm build]: https://jujucharms.com/docs/stable/authors-charm-building


## Provided Relations

### hdfs (interface: [hdfs][])

This relation connects this charm to the charms which require a NameNode,
such as the apache-hadoop-resourcemanager and apache-hadoop-plugin charms.

Information on this interface can be found at [interface-hdfs][hdfs]


### ganglia (interface: [monitor][])

This relation connects this charm to the [Ganglia][] charm for monitoring.
Information on this interface can be found at [interface-monitor][monitor]

[hdfs]: https://github.com/juju-solutions/interface-hdfs
[monitor]: https://github.com/juju-solutions/interface-monitor
[Ganglia]: https://jujucharms.com/ganglia/
