## Overview

The Apache Hadoop software library is a framework that allows for the
distributed processing of large data sets across clusters of computers
using a simple programming model.

This charm deploys an HDFS master node running the NameNode component of
[Apache Hadoop 2.7.1](http://hadoop.apache.org/docs/r2.7.1/), which manages
the distribution and replication of data among the various DataNode components.

## Usage

This charm is intended to be deployed via one of the
[apache bundles](https://jujucharms.com/u/bigdata-charmers/#bundles).
For example:

    juju quickstart apache-analytics-sql

This will deploy the Apache Hadoop platform with Apache Hive available to
perform SQL-like queries against your data.

You can also manually load and run map-reduce jobs via the plugin charm
included in the bigdata bundles linked above:

    juju scp my-job.jar plugin/0:
    juju ssh plugin/0
    hadoop jar my-job.jar

## Benchmarking

This charm provides several benchmarks to gauge the performance of your
environment.

The easiest way to run the benchmarks on this service is to relate it to the
[Benchmark GUI][].  You will likely also want to relate it to the
[Benchmark Collector][] to have machine-level information collected during the
benchmark, for a more complete picture of how the machine performed.

[Benchmark GUI]: https://jujucharms.com/benchmark-gui/
[Benchmark Collector]: https://jujucharms.com/benchmark-collector/

However, each benchmark is also an action that can be called manually:

        $ juju action do namenode/0 nnbench
        Action queued with id: 55887b40-116c-4020-8b35-1e28a54cc622
        $ juju action fetch --wait 0 55887b40-116c-4020-8b35-1e28a54cc622

        results:
          meta:
            composite:
              direction: asc
              units: secs
              value: "128"
            start: 2016-02-04T14:55:39Z
            stop: 2016-02-04T14:57:47Z
          results:
            raw: '{"BAD_ID": "0", "FILE: Number of read operations": "0", "Reduce input groups":
              "8", "Reduce input records": "95", "Map output bytes": "1823", "Map input records":
              "12", "Combine input records": "0", "HDFS: Number of bytes read": "18635", "FILE:
              Number of bytes written": "32999982", "HDFS: Number of write operations": "330",
              "Combine output records": "0", "Total committed heap usage (bytes)": "3144749056",
              "Bytes Written": "164", "WRONG_LENGTH": "0", "Failed Shuffles": "0", "FILE:
              Number of bytes read": "27879457", "WRONG_MAP": "0", "Spilled Records": "190",
              "Merged Map outputs": "72", "HDFS: Number of large read operations": "0", "Reduce
              shuffle bytes": "2445", "FILE: Number of large read operations": "0", "Map output
              materialized bytes": "2445", "IO_ERROR": "0", "CONNECTION": "0", "HDFS: Number
              of read operations": "567", "Map output records": "95", "Reduce output records":
              "8", "WRONG_REDUCE": "0", "HDFS: Number of bytes written": "27412", "GC time
              elapsed (ms)": "603", "Input split bytes": "1610", "Shuffled Maps ": "72", "FILE:
              Number of write operations": "0", "Bytes Read": "1490"}'
        status: completed
        timing:
          completed: 2016-02-04 14:57:48 +0000 UTC
          enqueued: 2016-02-04 14:55:14 +0000 UTC
          started: 2016-02-04 14:55:27 +0000 UTC


## Status and Smoke Test

The services provide extended status reporting to indicate when they are ready:

    juju status --format=tabular

This is particularly useful when combined with `watch` to track the on-going
progress of the deployment:

    watch -n 0.5 juju status --format=tabular

The message for each unit will provide information about that unit's state.
Once they all indicate that they are ready, you can perform a "smoke test"
to verify that HDFS is working as expected using the built-in `smoke-test`
action:

    juju action do namenode/0 smoke-test

After a few seconds or so, you can check the results of the smoke test:

    juju action status

You will see `status: completed` if the smoke test was successful, or
`status: failed` if it was not.  You can get more information on why it failed
via:

    juju action fetch <action-id>


## Monitoring

This charm supports monitoring via Ganglia.  To enable monitoring, you must
do **both** of the following (the order does not matter):

 * Add a relation to the [Ganglia charm][] via the `:master` relation
 * Enable the `ganglia_metrics` config option

For example:

    juju add-relation namenode ganglia:master
    juju set namenode ganglia_metrics=true

Enabling monitoring will issue restart the NameNode and all DataNode components
on all of the related compute-slaves.  Take care to ensure that there are no
running jobs when enabling monitoring.


## Backup / HDFS export

This charm has an action called 'backup-hdfs' which will perform a distcp to
a destination hadoop namenode. By default, the entire hdfs fs will be copied
with the -update flag, meaning any non-existing files will be copied, and
any different files will be overwritten. If the destination namenode is another
juju deployment, be sure to run the command: 'juju expose namenode/?'. Otherwise
make sure any firewalls are opened.

  * The 'dest' param is mandatory and contains an ip or resolvable hostname for
the destination hadoop namenode.

Example usage:

    juju action do namenode/0 backup-hdfs dest='10.0.0.100'

This will run the following hadoop command as the hdfs user:

     hadoop distcp -update hdfs://namenode-0:8020/ hdfs://10.0.0.100:8020/o

Other supported params are:

  * srcpath: Specify a different source path to the default of /
  * srcport: Specify a different HDFS port to the default of 8020
  * destpath: Equivalent to srcpath for the destination
  * destport: Equivalent to srcport for the destination
  * update: Boolean, defaults to true, update files and create new files
  * overwrite: Boolean, defaults to false, overrides update, copies everything
  * options: Additional command line params to pass to the hadoop distcp command

If both update and overwrite are false, a standard distcp job will run.


Refer to apache distcp docs for further details:
[Hadoop 2.7.2 distcp docs](https://hadoop.apache.org/docs/r2.7.2/hadoop-distcp/DistCp.html).


## Deploying in Network-Restricted Environments

The Apache Hadoop charms can be deployed in environments with limited network
access. To deploy in this environment, you will need a local mirror to serve
the packages and resources required by these charms.


### Mirroring Packages

You can setup a local mirror for apt packages using squid-deb-proxy.
For instructions on configuring juju to use this, see the
[Juju Proxy Documentation](https://juju.ubuntu.com/docs/howto-proxies.html).


### Mirroring Resources

In addition to apt packages, the Apache Hadoop charms require a few binary
resources, which are normally hosted on Launchpad. If access to Launchpad
is not available, the `jujuresources` library makes it easy to create a mirror
of these resources:

    sudo pip install jujuresources
    juju-resources fetch --all /path/to/resources.yaml -d /tmp/resources
    juju-resources serve -d /tmp/resources

This will fetch all of the resources needed by this charm and serve them via a
simple HTTP server. The output from `juju-resources serve` will give you a
URL that you can set as the `resources_mirror` config option for this charm.
Setting this option will cause all resources required by this charm to be
downloaded from the configured URL.

You can fetch the resources for all of the Apache Hadoop charms
(`apache-hadoop-namenode`, `apache-hadoop-resourcemanager`,
`apache-hadoop-slave`, `apache-hadoop-plugin`, etc) into a single
directory and serve them all with a single `juju-resources serve` instance.


## Contact Information

- <bigdata@lists.ubuntu.com>


## Hadoop

- [Apache Hadoop](http://hadoop.apache.org/) home page
- [Apache Hadoop bug trackers](http://hadoop.apache.org/issue_tracking.html)
- [Apache Hadoop mailing lists](http://hadoop.apache.org/mailing_lists.html)
- [Apache Hadoop Juju Charm](http://jujucharms.com/?text=hadoop)
