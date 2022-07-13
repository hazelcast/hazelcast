# Split-brain Recovery Improvements

|ℹ️ Since: 5.2|
|-------------|

### Table of Contents

+ [Background](#background)
  - [Description](#description)
  - [Terminology](#terminology)
+ [Functional Design](#functional-design)
  * [Summary of Functionality](#summary-of-functionality)
  * [Additional Functional Design Topics](#additional-functional-design-topics)
    + [Notes/Questions/Issues](#notesquestionsissues)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)



|||
|---|---|
|Related Jira|_https://hazelcast.atlassian.net/browse/HZ-922_|
|Related Github issues|_https://github.com/hazelcast/hazelcast/issues/17489_ _https://github.com/hazelcast/hazelcast/issues/17490_ _https://github.com/hazelcast/hazelcast/issues/18661_ _https://github.com/hazelcast/hazelcast/issues/20331_|
|Document Status / Completeness|DRAFT|
|Requirement owner|_Jiri Holusa_|
|Developer(s)|_Ufuk Yılmaz_|
|Quality Engineer|_TBD_|
|Support Engineer|_Support Engineer_|
|Technical Reviewers|_Josef Cacek_, _2nd reviewer TBD_|
|Simulator or Soak Test PR(s) |_Link to Simulator or Soak test PR_|

### Background
#### Description

There are some mismatches between current cluster join and split-brain merge
protocols and in some cases, those inconsistencies result in the issues that the
Hazelcast members which initially can form a cluster (join to each other) are
not able to recover from the split-brain even though the network failure which
caused the split-brain is healed. This is because the master member of the
smaller cluster fails to discover the members of the other bigger cluster, so
the split-brain merge process never begins.

To explain this reason for this failure in a little more detail, the master
member of the smaller cluster, which is supposed to initiate split-brain merge
from the smaller cluster to the bigger cluster, does not contain the address of
any member of the larger cluster in the member list of its join configuration.
According to the current split-brain merge protocol, searching for the other
cluster is only performed by the master members of the sub-clusters, and the
master member performs its search by only using the member addresses listed in
its join config. This split-brain healing protocol is designed by assuming that
addresses of all members are listed in the join config of all other members,
therefore the master member of the smaller cluster is supposed to reach the
members of other clusters just by looking at its member list without asking any
other members. But, this assumption does not hold for all kinds of cluster
formations. For example, consider a scale-up situation, we add new members after
some time since the number of members that we started at the beginning is
insufficient, it is possible that the addresses of these new members do not
exist in the configurations of the first set of members, since we will not have
added these new members' addresses in the configurations of the first set of
members while starting them. In a cluster with a long lifecycle, some members
may not have the address of every other member, thus we can encounter an
unrecoverable split-brain condition in such a case.

In the cluster join protocol, a new member, which only contains the subset of
the members in its join config, can ask the addresses of other members to the
reachable members that it can connect and find the other members which are not
listed in its network join config by communicating with the reachable members.
But, this is not the case in the split-brain merge. Only the master member
performs this search and it could be possible that it cannot find any member of
the other cluster when it only uses its initial knowledge (knowledge -> initial
the member list in its network join config). Thus, if the address of any member
of the other cluster is not listed in this master member's join configuration,
then this cluster cannot recover from the split-brain. This master member never
discovers the existence of the other cluster, and the split-brain merge never
begins.

<!--
In this comment block, we describe how the cluster join of Hazelcast members
 happens:
- A new member starts and tries to connect to the member addresses it
  has in its join configuration's member list or discovered by using
  other mechanisms one by one.
- If this new member can manage to connect any member existed in its
  join config's member list, it asks for the master member of the
  cluster that the reachable member belongs to and after this member
  gets reply for its `WhoIsMasterOperation`, it finds the master member
  by communicating this member. (If no master member is already
  selected, they choose a master with a mastership claim later
  on. I will not describe this process for the sake of simplicity.)
- Then, this new member connects to the master member of the cluster and
  then gets the member list of all other joined members from the master
  member, then it connects to all other members with using this member
  list.
-->

In the scope of this task, we aim to solve all the split-brain recovery issues
caused by failing to discover the master member of other cluster.

#### Motivation

We want to improve the resiliency and stability of the Hazelcast cluster by
enabling it to recover split-brain conditions even in certain corner cases.
Until now, some of the customers had problems with that this split-brain cluster
merge process did not complete, and we want to eliminate such issues. That
problem occurrences show that the cluster configurations in these problematic
cases are reasonable; users have sensible reasons to configure the cluster as
they did and these issues are not caused by misconfiguration so providing
support for these setups is valuable for us.

#### Goals

- Improve split-brain healing mechanism to cover under even certain corner cases
- Ensure that the cluster recover from the split-brain in every cluster setup that can be
  initially formed

### Functional Design

#### Summary of Functionality

This task only contains split-brain recovery improvements. There is no
planned functional changes.

#### User Interaction
There is no planned API change with this task.

#### Technical Design

#### Proposed Solution 1

We can register the public addresses of the members obtained with the member
list from master to some registry which can be accessible from the cluster join
manager. Then, we can look up these member addresses from this registry during
the cluster discovery of the split-brain handler. There are some problems with
this approach:
- We don't know when to remove/clean up the registered addresses from this
  registry. When having a dynamic cluster setup having constant member additions
  and removals with different addresses, these registered addresses can
  accumulate a lot which may slow down the execution of the split-brain handler.
- It does not solve the situations where there are members who have not yet
  joined each other. See: https://github.com/hazelcast/hazelcast/issues/18661

#### Proposed Solution 2

Provide ways to users to dynamically update the member list of the tcp-ip join
configuration. We plan to support the ways listed below:
- via newly introduce REST endpoint:
- via already existing [configuration reload and update endpoints](https://docs.hazelcast.com/hazelcast/5.1/configuration/dynamic-config-update-and-reload#update) which requires us integrating this configuration element with the dynamic configuration update. 

#### Proposed Solution 3 (Not implementing)

In this solution, aligning the member discovery protocol in this split
brain handler with that of the cluster join mechanism and want to turn it into a
discovery process that all the cluster member participates in. Thus, the members
who can join to each other, will be able to discover each other when there is a
split-brain. Also, make the bigger cluster discovers the small cluster
be enough to initiate this split-brain recovery process.

Downsides of this approach:
- This solution may require to change the split-brain discovery protocol, it
  can be difficult to change it in a backward compatible way.
- Its efficient implementation is like solving a graph problem and more
  difficult than the second approach
- Depending on only the initial member list knowledge for the recovery purposes
 is not sufficient to recover from all of the split brain scenarios.

#### Testing Criteria

Add unit and integration tests to verify that the split brain recovery
is performed after the split brain condition removed.