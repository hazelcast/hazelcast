---
title: Scale Your Job
description: How do Jet clusters deal with nodes failing or being added and removed intentionally. 
---

Now that we have a running job on the cluster, we can see some
non-trivial features of Hazelcast Jet in action.

One of the main advantages of using a distributed streaming engine like
Jet the ability to dynamically scale a job up or down. Jet keeps
processing data without loss even when a node fails, and you can add
more nodes that immediately start sharing the computation load.

First, let's make sure that the job from the previous section is still
running:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->

```bash
$ bin/jet list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
03de-e38d-3480-0001 RUNNING            2020-02-09T16:30:26.843 N/A
```

<!--Docker-->

```bash
$ docker run -it hazelcast/hazelcast-jet jet -t 172.17.0.2 list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
03e3-b8f6-5340-0001 RUNNING            2020-02-13T09:36:46.898 N/A
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Start a second Jet node

Now let's open another terminal window and start another Jet node:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->

```bash
bin/jet-start
```

<!--Docker-->

```bash
docker run hazelcast/hazelcast-jet
```

<!--END_DOCUSAURUS_CODE_TABS-->

You can observe in the logs of the original Jet instance that it
discovered the new node, formed a cluster with it, and then went on to
restart the job so it runs on both cluster members. You should notice
log lines similar to these:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->

```text
Members {size:2, ver:2} [
    Member [192.168.0.2]:5701 - 7717160d-98fd-48cf-95c8-1cd2063763ff
    Member [192.168.0.2]:5702 - 5635b256-b6d5-4c88-bf45-200f6bf32104 this
]
```

<!--Docker-->

```text
Members {size:2, ver:2} [
    Member [172.17.0.2]:5701 - 4bc3691d-2575-452d-b9d9-335f177f6aff
    Member [172.17.0.3]:5701 - 7d07aad7-4a22-4086-a5a1-db64cf664e7d this
]
```

<!--END_DOCUSAURUS_CODE_TABS-->

Congratulations, you have created a cluster of two nodes!

>If for some reason your nodes didn't find each other, this is likely
because multicast is turned off or not working correctly in your
environment. In this case, please see the
[Configuration](../operations/configuration) section as this will
require you to use another cluster discovery mechanism than the
default, which is IP multicast.

The job is now running on both nodes, but the log output is still
appearing on just one of them. This is because the test data source is
non-distributed, and we don't have any steps in the pipeline which
require data rebalancing. We will explain this later in detail in the
[distributed execution](../architecture/distributed-computing) section.

Another thing you may notice here is that the sequence numbers were
reset to zero, this is because the test source we're using is not
fault-tolerant. This is explained in detail in the [fault
tolerance](../concepts/processing-guarantees) section.

## Terminate one of the nodes

We can also do the reverse: simply kill one of the nodes with Ctrl-C or
close its terminal window. The job will restart automatically and keep
running on the remaining nodes.

## Next Steps

You have now successfully deployed and scaled your first distributed
data pipeline. The next step is getting a deeper understanding of the
[Pipeline API](../api/pipeline) which will allow you to write more complex
data transforms.
