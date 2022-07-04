# Cluster Service Consistency Improvements

|ℹ️ Since: 3.9| 
|-------------|
  

## Background

### Description

There are a few consistency issues related to membership update (member
join and removal) process. These are;

1.  Cluster membership update operations are not ordered, new member
    addition and removal operations can get reordered on
    receiving/processing side. Also periodic member-list publish
    operation has no order with other member adding/removing operations.
    That can cause having different member lists on different members.
    For example assume cluster has \[A, B, C\] members;

    -   B shuts down

    -   A publishes \[A, B, C\] with a periodic member-list publish
        operation

    -   B is removed from both A and C. Member-list seen by;

        -   A -\> \[A, C\]

        -   C -\> \[A, C\]

    -   Periodic member-list publish operation is processed by C.
        Member-list seen by;

        -   A -\> \[A, C\]

        -   C -\> \[A, B, C\]

    -   After that point all membership updates will be rejected by C.

2.  When a member remove message is missed by an existing cluster
    member, removal may not be realized until a heartbeat timeout
    occurs. Also, subsequent membership updates may get rejected.

3.  When master changes, if new master doesn't know the latest member
    list at that point, then it will overwrite the most recent member
    list with its stale version and members which are not present in new
    master's member list have to split completely and merge back later.

### Terminology

| Term                          | Definition                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Member list version           | A monotonic version number to identify freshness of a specific member list.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Master member                 | Oldest member in the cluster which is responsible for administrating the cluster                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Slave member                  | Non-master members of the cluster. Sorry for the terminology. Hazelcast slaves are cool guys since they don't do any work until they become master.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Suspicion                     | The situation in which a **slave** thinks that **another member** (i.e., the master or another slave) of the cluster **may be** dead. It can occur because of a heartbeat timeout or a connection problem. **Suspicions are done based on local information.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Master confirmation           | The message periodically sent **from a slave to the master** to notify that the slave still follows its master                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Master confirmation timeout   | If **the master node** doesn't receive a master confirmation message **from a slave** for "master confirmation timeout" duration, it will kick that slave from the cluster.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Heartbeat                     | The message periodically sent **between all members** to sustain their presence.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Heartbeat timeout             | If **a member** doesn't receive a heartbeat message **from another member** for "heartbeat timeout" duration, there are two cases. If the receiver is the master node, it will kick the silent slave from the cluster. If the receiver is the slave node, it will only suspect that the silent member may be dead.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Explicit suspicion            | There are certain situations in which there is a partial split in the cluster and members want to convert it to a full split to be able to recover from the split brain case. For instance, master node kicks an alive slave from the cluster for some reason, but the slave is not aware of it yet. In that case, slave will eventually send a heartbeat or a master confirmation message. Then, master node replies to the slave as follows: **Hey, I am still alive but just assume that I am dead and go your own way. We will meet back eventually.** This message is called **explicit suspicion**. When a member receives an explicit suspicion message, it will start a suspicion about the sender and continue its progress. We call this message "explicit suspicion" because it states a fact, which is learnt from another member, in opposite to the local suspicions, which are based on only local information. |
| Failure Detector              | Failure detector of a node decides whether a node failed/crashed by analyzing several factors, such as; heartbeat timeout, master confirmation timeout, network disconnection etc.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Older member / younger member | In the member list \[ A, B, C \], A is older than B and C, B is older than C. Similarly, C is younger than A and B, B is younger than A.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

## Functional Design

There will no functional changes from user point of view. Membership
update process will work very similar, just internal infrastructure will
be more consistent and safe.

## User Interaction

There will be no updates/changes in public api and configuration. Two
new system properties will be added;

-   `hazelcast.mastership.claim.timeout.seconds`: The timeout which
    defines when master candidate gives up waiting for response to its
    mastership claim. After timeout happens, non-responding member will
    be removed from member list. See [mastership claim process](#mastership-claim-process)
    for details.  
     

-   `hazelcast.legacy.memberlist.format.enabled`: Enables the legacy,
    pre 3.9, member list format in logs. New format will include member
    list version too.   
      

    -   Legacy format:

        ```
        Members [3] {
            Member [127.0.0.1]:5701 - c1ccc8d4-a549-4bff-bf46-9213e14a9fd2 this
            Member [127.0.0.1]:5702 - 33a82dbf-85d6-4780-b9cf-e47d42fb89d4
            Member [127.0.0.1]:5703 - 813ec82f-9d9e-4712-bae1-6c95b32d6d7d
        } 
        ```

          

    -   New format:

        ```
        Members {size:3, ver:3} [
            Member [127.0.0.1]:5701 - e40081de-056a-4ae5-8ffe-632caf8a6cf1 this
            Member [127.0.0.1]:5702 - 93e82109-16bf-4b16-9c87-f4a6d0873080
            Member [127.0.0.1]:5703 - 06fb4e61-9757-443b-a19f-7af1f3966f30
        ]
        ```

          

Additionally, periodic member list publish interval
(`hazelcast.member.list.publish.interval.seconds)` will be downed from
300 seconds to 60 seconds.

## Technical Design

### Theory

We borrow some ideas from the [Group Membership and View Synchrony in Partitionable Asynchronous Distributed Systems: Specifications](https://dl.acm.org/doi/pdf/10.1145/250007.250010) 
paper and [Jgroups](http://jgroups.org/) project to introduce new rules
and invariants into our cluster management system. Jgroups is also an
implementation of the paper. The paper already fits to our system
because it maintains a "partitionable" system, in which members of the
cluster can split into partitions, and it assumes the crash-stop model.
Both are already handled by Hazelcast.

We deal with the "group membership problem", which is defined in the
paper as following:

> "Informally, a group is a set of processes that cooperate towards some
> common goal or share some global state that is distributed or
> replicated. The composition of the group is dynamic due to processes
> that voluntarily join and leave the computation, those that need to be
> excluded due to failures and those that need to be included after
> repairs. **A group membership service tracks these changes and
> transforms them into views that are agreed upon as defining the
> group's current composition.**"

In Hazelcast terms, the master node is responsible for performing the
group membership service, as it manages the cluster member list. Once a
member joins or leaves the cluster, a new view (i.e., member list) is
created and publish it to the slaves. 

Process or communication link failures may cause group membership to
split across multiple partitions. Similarly, due to the asynchrony of
the system (i.e., unbounded gc pauses, network delays, etc.), members of
a cluster can also create "virtual" partitions, which are
indistinguishable from real ones. It means that there can be multiple
views (i.e., network partitions) present in the system concurrently.
This model is equivalent to behaviour of Hazelcast clusters. There can
be multiple non-intersecting Hazelcast clusters in the same environment.
They work as separate clusters until they eventually discover each other
and merge into a single cluster.

The paper introduces 3 properties a "partitionable group membership
service" needs to have:

**M1 (View Order):**

> If two views v and w are installed in a given order at some process p,
> then v and w cannot be installed in a conflicting order at some other
> process q.

In Hazelcast terms, each Hazelcast member should apply the member list
updates in the same order. 

  

**M2 (View Agreement):**

> If a correct process p installs view v, then for every process q in v,
> either (i) q eventually installs v, or (ii) p eventually installs view
> w as a immediate successor to v such that q not in w.

In Hazelcast terms, lets say we have two nodes: A, B. Both nodes have
the member list: \[ A, B \]. A new member C joins to cluster and master
node publishes a new member list \[ A, B, C \]. Then, B should either
apply \[ A, B, C \] or split from the cluster, and A publishes a new
member list: \[ A, C \].

  

**M3 (View Integrity):**

> Every view installed by a process includes itself.

This property is to eliminate trivial solutions.

  

With the improvements implemented in this work, Hazelcast satisfies all
the properties describe above. Member list updates are performed in the
master node and they are ordered with a monotonic version number. Slaves
apply member list updates they receive from the master and they check
version numbers in member lists to apply them in the correct order. On
mastership changes, slaves ensure that monotonicity of the member list
version is preserved. 

  

### Membership Update Process

Membership updates (member join & removal) are performed by only master
node. During join process, master handles join requests and decides
whether requester is able to join the cluster.  When join request is
accepted, new node is added to the member list and member list version
is incremented. Then the new member list is published to the cluster. 

When master node decides/detects a member as dead, that member is
removed from the member list and the new member list is published to the
cluster with incremented member list version. 

Non-master nodes do not accept join requests and do not decide member
removals by themselves. When they receive a member list update message,
they either apply the new member list if incoming version is greater
than the locally known version or just ignore the update if incoming
version is less than or equal to local one. Version number of received
member list can be less than or equal to locally known version when;

-   membership update messages are received/processed in out-of-order

-   a periodic member list update is received when there's no actual
    member list change

Membership updates can be received/processed out-of-order when multiple
members join in parallel or when a periodic member list publish is
received simultaneously with a membership update. This out-of-order
execution does not do any harm in the system, since monotonic member
list version attached to each update message guards against applying a
stale update.

Also, it's possible to miss an intermediate membership update, because
of network interrupts or packet losses. When a new membership update
message arrives, either because of a newly joining member or a periodic
member list publish, lagging node eventually learns the latest member
list and version. 

### Member Suspicion & Removal

In the previous implementation, when a Hazelcast member encounters
failure of another member (by the decision of failure detector),
regardless of being master or not, it directly removes the problematic
member from its member list. This standalone member removal decision
making can easily lead to divergence of member list between cluster
members.

In the new design, when failure detector of a member suspects that
another member might be failed:

-   on master node, the failed member is removed immediately and a new
    member list update is published to the cluster. 

-   on a slave node, failed member is marked as suspected. Slaves do not
    share their local suspicions with each other. 

When a member is suspected, heartbeat and master-confirmation messages
are not sent to that member. 

#### Explicit Suspicion

In some cases, even though failure detector does not suspect from a
member (there are no communication failures, no heartbeat issues etc),
members can request to be suspected by another member. Reason behind is,
member thinks that their view of cluster is already diverged because of
earlier undetected communication failures.

Cases where a member sends explicit suspicion request are;

-   when a master receives master-confirmation message from an unknown
    endpoint

-   when a master receives heartbeat message from an unknown endpoint

-   when a member receives membership update from a non-master member

-   when a non-joined member receives finalize join message from an
    invalid master

When a non-master member receives heartbeat message from an unknown
endpoint, it sends a complaint message to master and master receiving a
complaint explicit suspicion messages to either of one or both of
complainee and complainer, according to its own verification.

#### Removing Suspicion

A slave member removes suspicion of the master when it receives a member
list update or a heartbeat from the master. Similarly, it removes
suspicion of another slave when it receives a heartbeat message from
that member. 

### Mastership Claim Process

If a slave suspects all members older than itself in the member list, it
starts the mastership claim process. 

There are 3 cases where master address of a node is updated:

-   When a starting member receives a `FinalizeJoinOp` to its previous
    join request.

-   When a slave suspects from all members older than itself, and
    decides to claim its own mastership. For instance, in the member
    list of \[ A, B, C, D \], when C suspects from A and B, it thinks
    that it should continue as the new master, and hence start the
    mastership claim process by first updating its master address to C. 

-   When a slave receives a mastership claim from an older slave, and
    agrees with that mastership claim. For the case above, D accepts the
    mastership claim of C only if it also suspects A and B. Otherwise,
    it makes C retry its mastership claim request to D. When D accepts
    mastership claim of C, it updates its master address to C. 

  

When a slave starts its mastership claim, it sends a mastership claim
operation to each of "non-suspected" member in its member list, younger
than itself. As a response to mastership claim, members send their
member lists. After collecting responses, mastership claiming member
creates a new member list with a new version and finalizes the process.
It puts the members who accepted its mastership claim into the final
member list.

  

**Example:**

We have 6 members: A, B, C, D, E, F

**`A:`**`  v = 6: [ A, B, C, D, E, F ]`

**`B:`**`  v = 4: [ A, B, C, D ], suspected =  [ C ]`

**`C:`**`  v = 6: [ A, B, C, D, E, F ]`

**`D:`**`  v = 5: [ A, B, C, D, E ]`

**`E:`**`  v = 6: [ A, B, C, D, E, F ]`

**`F:`**`  v = 6: [ A, B, C, D, E, F ]`

In this scenario, B has missed 2 member list updates (v=5, v=6) and D
 has missed the last member list update: v=6. Lets assume B also
suspects C for some reason, and F is non-responsive.

When A crashes, B eventually suspects from A and checks if it should
start a mastership claim process. Since there is no older member who
should take the mastership, B starts the mastership claim process. The
rest of the process is as follows:

-   B doesn't send mastership claim to C since it is already suspected.

-   B sends a mastership claim to D and receives the
    response: `{v=5, member list = [A, B, C, D, E]}`. When it checks the
    response, it discovers that there is new member: E in the received
    member list.

-   B sends a mastership claim to E and receives the
    response: `{v=6, member list = [A, B, C, D, E, F]}`. Similarly, it
    discovers F. 

-   B sends a mastership claim to F. Somehow, F is also having issues
    and doesn't respond to B's mastership claim. 

After these steps, B notices that it has asked every other it already
knows about. It already suspects C, it knows that D and E accepted its
mastership claim, and could not get a response from F. Final member list
consists of only B, D, E. Then B completes mastership claim process by
publishing final member list by incrementing the
version: `{v=7, member list = [B, D, E]}`. 

##  Testing Criteria

Added 3.8 \~ 3.9 compatibility
tests: [hazelcast-enterprise/pull/1449](https://github.com/hazelcast/hazelcast-enterprise/pull/1449)  
 
