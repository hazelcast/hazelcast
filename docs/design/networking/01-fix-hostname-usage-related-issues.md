# Fix hostname usage related issues

### Table of Contents

+ [Background](#background)
    - [Description](#description)
    - [Terminology](#terminology)
    - [Actors and Scenarios](#actors-and-scenarios)
+ [Functional Design](#functional-design)
+ [User Interaction](#user-interaction)
    - [API design and/or Prototypes](#api-design-andor-prototypes)
+ [Technical Design](#technical-design)
    - [Notes/Questions/Issues](#notesquestionsissues)
+ [Testing Criteria](#testing-criteria)


|||
|---|---|
|Related Jira| https://hazelcast.atlassian.net/browse/HZ-528       |
|Related Github issues          | https://github.com/hazelcast/hazelcast/issues/15722, https://github.com/hazelcast/hazelcast/issues/16651, https://github.com/hazelcast/hazelcast-enterprise/issues/3627, https://github.com/hazelcast/hazelcast-enterprise/issues/4342 |
|Implementation PR|https://github.com/hazelcast/hazelcast/pull/20014|
|Document Status / Completeness | DESIGN REVIEW|
|Requirement owner              | _Requirement owner_                                 |
|Developer(s)                   | _Ufuk Yılmaz_                                       |
|Quality Engineer               | _Viacheslav Sytnyk_                                 |
|Support Engineer               | _Support Engineer_                                  |
|Technical Reviewers            | _Vassilis Bekiaris_, _Josef Cacek_                  |
|Simulator or Soak Test PR(s)   | _Link to Simulator or Soak test PR_                 |

### Background

#### Description

Several problems can arise when using hostnames in Hazelcast's network
and WAN configurations. With this effort, we aim to provide a seamless
usage of hostnames in the Hazelcast configurations by fixing issues
related to hostname usage. The source of the problems is the fact that
multiple network addresses can point to the same member, and we ignore
this fact while dealing with network addresses in our codebase. A
significant part of our codebase is written assuming a member will only
have one network address, but this assumption fails, so we encounter
errors/wrong behavior in the different processes of the Hazelcast.

When using hostnames in the network configuration of the members, we
directly encounter the situation of having multiple addresses for the
members as a consequence of using hostnames. Because when we use
hostnames, the members know both the IP address of the member by
resolving the hostname and the raw hostname address for this member.
Then, issues arise because members don't properly manage these multiple
addresses.

In this effort, we will try to manage properly these multiple network
addresses of the members and this will resolve the hostname usage
related issues.

#### Motivation

We want to provide a seamless hostname usage experience to our users.
Until now, many of our users and customers have had problems with this
regard as shown in the known issues. Although the hostname usages can be
replaced with IP based addressing in simple networking setups, it is a
necessary functionality for maintaining complex network setups where IPs
are dynamically assigned to the machines that Hazelcast members will
work on. This is specifically important for cloud-based deployments that
could be the subject of more complex networking setups compared to on
premise deployments that may include members whose IPs are assigned
dynamically and these addresses identified by DNS based hostname
resolution mechanism.

#### Goals
- Support hostname-based addressing in member-to-member communication
- Support hostname-based addressing in WAN configuration
- Support the hostname-based addressing in relevant discovery plugins
  (e.g. discovery in AWS, Azure, GCP, Kubernetes)

In summary, we want Hazelcast to work when a hostname address used
instead of an IP address in any Hazelcast cluster configuration that
works when an IP address is used in the configuration element. In short,
we want any cluster setup that works properly when IP addresses are
used, to operate when hostnames are used.

#### Terminology

|Term|Definition|
|---|---|
|Hostname|A hostname is an easy to use name assigned to a host machine on a network which is mapped into an IP address/IP addresses (We will not consider hostnames mapped to multiple IP addresses, as there is no such use case for it in Hazelcast).|
|Hostname resolution|The process of converting the hostname to its mapped IP Address so that socket bind and connect can be performed on this IP address|

#### Actors and Scenarios

#### Scenario 1 - Hazelcast user must be able to use hostnames in member server bind(publicAddress) and join configuration:

1. User configures members' join and bind configuration with using hostnames and starts members.
2. These members must be able to join and form cluster.
3. Any other Hazelcast functionalities depending on networking must
   continue to work correctly. (Since Hazelcast is a distributed system,
   networking changes concern many parts of it)

#### Scenario 2 - Hazelcast user must be able to use hostname in WAN target address configuration:

1. User configures members' wan target configuration with using hostnames and starts members.
2. The WAN members must be able to connect each other
3. Any other Hazelcast functionalities depending on networking must
   continue to work correctly. (Since Hazelcast is a distributed system,
   networking changes concern many parts of it)


### Functional Design
There will be no functional changes.

### User Interaction
#### API design and/or Prototypes

If users have previously used member addresses in their custom codes for
member identification purposes, they will still encounter address
management problems when their configurations include hostnames. We need
to provide them with a way to manage member addresses. So, for solving
this kind of problem, we should expose our managed `UUID-Set<Address>`
and its reverse mappings to our users. The jet connectors as a user of
Hazelcast member APIs uses some `Address` dependent logic inside. e.g.
In Elasticsearch connector, we use member addresses to check if in case
the Hazelcast cluster runs on the same machines as Elastic it uses the
addresses to plan the job in a way the reading from the local shard that
belongs to same machine and doesn't go over the network. This is also
same for the hadoop connectors. This logic may go wrong when Hazelcast
members have multiple addresses.

Up until now, our users mainly use the picked public address of the
members for member identification purposes in the member logs. We should
not make any big changes in the member logs so as not to confuse our
users. We should store the address configured by the user as the primary
address and continue to print it in the member logs. Even if we do the
member identification with a UUID-based logic, we should keep the member
address in related places to show it in the logs.

### Technical Design

What we propose as a solution to these issues is that we will try to
manage this multiple network addresses at the Hazelcast networking
level, and we will avoid dealing with multiple addresses in high-level
places by only exposing single representation of these addresses to the
higher levels. We'll call this single exposed address the primary
address of the member. We select this primary address as follows:
- For the member protocol connections and for other connections using unified
  connection manager, we select the public address of the member protocol which
  corresponds to `com.hazelcast.instance.ProtocolType#MEMBER` as the primary
  address.
- For the other protocol connections (this is possible when advanced networking
  is enabled.), we select the public address of the corresponding protocol as the
  primary address. e.g. we select the public address that corresponds to
  `com.hazelcast.instance.ProtocolType#WAN` for the incoming connections to the
  connection manager which manages only the wan connections.

After exposing only the primary address to those high-level places and
after making sure its singularity, we only need to manage multiple
addresses in the network/connection manager layer. In the connection
manager, we will manage with multiple member addresses and their
connections using the members' unique UUIDs.

Although there is only one selected public address of the member
corresponds to each EndpointQualifier of this member, it is possible to
reach these endpoints using addresses other than this defined public
addresses. It is impossible to understand that these different addresses
point to the same member just only looking at the content of these
address objects. Even if we only use the IP addresses, multiple
different IP addresses can refer to the same host machine. Hazelcast
server socket can bind different network interfaces of the host machine
which can have multiple private and public addresses etc.; or consider
that adding network proxies to the front of the member, the addresses of
these proxies behave as the addresses of the member on the viewpoint of
connection initiator. These public/private addresses or proxy addresses
are completely different in their contents. Also, multiple hostnames can
be defined in DNS to refer to the same machine (we may understand these
hostnames point to the same member if they resolve to the same IP
address, but how to decide when to do this resolution, and they don't
always have to resolve to the same IPs .), and so we don't have a chance
to understand these addresses referring to the same member just only
looking at their contents. To understand this, we need to connect the
remote member and process the `MemberHandshake` of this remote member.
In the member handshake, a member receive the member UUID of the remote
member, and after that, we can understand the connected address belongs
to which member. We are registering this connected address as the
address of the corresponding member UUID during this `MemberHandshake`
processing. In this `MemberHandshake`, the remote member also shares its
public addresses, and we also register the public addresses of this
member under the member's unique UUID together with the connected
address as aliases of each other.


To manage the address aliases, we create an Address registry to store
`UUID -> Set<Address>` and `Address'es -> UUID` mappings. Since we can
access the UUIDs of the members after getting the MemberHandshake
response, the entries of these maps will be created on connection
registration which is performed during the handshake processing. We
decide to remove the address registry entries when all connections to
the members are closed. While determining the address removal timing, we
especially considered the cases such as hot restart on a different
machine with the same UUID, member restarts, and split-brain merge;
these actions may cause the address registry entries to become stale. If
we tried to extend the lifetime of these map entries a little longer,
this would cause problems with Hot Restart with the same UUID that's why
we chose to stick the lifetime of registrations to the lifetime of
connections. While trying to create this design, we considered removing
all the `Address` usages from the codebase. But, this change was quite
intrusive since we use the addresses in lots of different places, and in
some places, it was not possible to remove them without breaking
backward compatibility. Also, we had a strict dependency on Address
usage in socket bind and socket connect operations by the nature; and we
cannot remove the addresses from these places. Since our retry
mechanisms in the operation service also try to reconnect the socket
channel if it's not available, an operation can trigger reconnect action
if we don't have an active connection to the address. For most of the
operations, we can have already established connection but for the join,
wan, and some cluster (heartbeat) operations, we depend on `connect`
semantics. But even the fact that few of the operations mentioned above
are dependent on connect semantics prevents us from easily removing the
Address usage from our operation invocation services. Also. in some
cluster service methods, lexicographic address comparisons are performed
between member addresses to determine some order among the members (when
deciding on a member to perform some task such as claiming mastership).
We don't have a chance to remove addresses from these places without
breaking backward compatibility.

Code examples that we cannot remove the addresses: 

```java
// When deciding on a member that will claim mastership after the first join
private boolean isThisNodeMasterCandidate(Collection<Address> addresses) {
    int thisHashCode = node.getThisAddress().hashCode();
    for (Address address : addresses) {
        if (isBlacklisted(address)) {
            continue;
        }
        if (node.getServer().getConnectionManager(MEMBER).get(address) != null) {
            if (thisHashCode > address.hashCode()) {
                return false;
            }
        }
    }
    return true;
}
```
```java
// When deciding on a member that will merge to target member after the split brain
private boolean shouldMergeTo(Address thisAddress, Address targetAddress) {
    String thisAddressStr = "[" + thisAddress.getHost() + "]:" + thisAddress.getPort();
    String targetAddressStr = "[" + targetAddress.getHost() + "]:" + targetAddress.getPort();

    if (thisAddressStr.equals(targetAddressStr)) {
        throw new IllegalArgumentException("Addresses must be different! This: "
                + thisAddress + ", Target: " + targetAddress);
    }

    // Since strings are guaranteed to be different, result will always be non-zero.
    int result = thisAddressStr.compareTo(targetAddressStr);
    return result > 0;
}
```

In the implementation, we defined a separate abstraction named
`LocalAddressRegistry` to manage the instance uuid's and corresponding
addresses. This registry keeps the registration count for the
registrations made on same uuid and using this registration count
mechanism, it removes this registry entry only when all connections to
an instance are closed. For the implementation details see:

 - [LocalAddressRegistry](https://github.com/hazelcast/hazelcast/blob/5.1-BETA-1/hazelcast/src/main/java/com/hazelcast/internal/server/tcp/LocalAddressRegistry.java)

- The new connection registration where we register uuid-address mapping entries: https://github.com/hazelcast/hazelcast/blob/5.1-BETA-1/hazelcast/src/main/java/com/hazelcast/internal/server/tcp/TcpServerConnectionManager.java#L197

- Connection close event callback where address registry entry deregistrations takes place for the member connections: 
https://github.com/hazelcast/hazelcast/blob/4a73cc5f9b4ebef09c5a8e0067da464b5ef629be/hazelcast/src/main/java/com/hazelcast/internal/server/tcp/TcpServerConnectionManagerBase.java#L284

- For the client connections: https://github.com/hazelcast/hazelcast/blob/4a73cc5f9b4ebef09c5a8e0067da464b5ef629be/hazelcast/src/main/java/com/hazelcast/client/impl/ClientEngineImpl.java#L443
#### Notes/Questions/Issues
1) Which address should take precedence while getting from
   `UUID-Set<Address>` map - IP or hostname? It seems like depending on
   the context, the precedence can be changed, e.g. we can prefer
   user-configured address in the member logs and TLS favorites using
   hostnames.
2) We had a strict dependency to Address usage in socket bind and
   connect operations. No matter how much we try to isolate the use of
   the addresses from upper places outside the networking level, in
   some operations we are dependent on bind/connect semantics. These
   are specifically join and WAN operations, and we couldn't isolate
   Addresses from them.
3) When we try to enforce UUID usage for member identification purposes,
   it can cause backwards compatibility issues. Removing the `Address`
   usages from our code base requires us to consider each changed place
   in terms of backwards compatibility. Since we replace addresses with
   uuid, we may need to convert serialized objects to send uuid instead
   of address, so we need to add RU compatibility paths for the
   serialization of those objects. These places will be resolved through
   the implementation. We need to document the changes in package by
   package manner. Questions to be answered: What is going to happen to
   use of `Address` in `com.hazelcast.xxx` package?
4) To note, we can use a new class to represent our aliases set,
   `Set<Address>` in our mappings, but I couldn't provide a good
   abstraction yet.

- Questions about the change:
    - How does this work in an on-prem deployment? How about on AWS and Kubernetes?
    - How does the change behave in mixed-version deployments? During a version upgrade? Which migrations are needed?
    In this effort, we avoid the changes that would break backward compatibility, so we
    don't expect any backward compatibility issue with respect to this change.
    - What are the possible interactions with other features or sub-systems inside Hazelcast? How does the behavior of other code change implicitly as a result of the changes outlined in the design document? (Provide examples if relevant.)
    - Is there other ongoing or recent work that is related? (Cross-reference the relevant design documents.)
    There is a previously reverted PR: https://github.com/hazelcast/hazelcast/pull/18591 (the related TDD is available in this PR changes), https://github.com/hazelcast/hazelcast/pull/19684
    - What are the edge cases? What are example uses or inputs that we think are uncommon but are still possible and thus need to be handled? How are these edge cases handled? Provide examples.
    - Mention alternatives, risks and assumptions. Why is this design the best in the space of possible designs? What other designs have been considered and what is the rationale for not choosing them?
    - Add links to any similar functionalities by other vendors, similarities and differentiators


- Questions about performance:
    - Does the change impact performance? How?
    - How is resource usage affected for “large” loads? For example, what do we expect to happen when there are 100000 items/entries? 100000 data structures? 1000000 concurrent operations?
    - Also investigate the consequences of the proposed change on performance. Pay especially attention to the risk that introducing a possible performance improvement in one area can slow down another area in an unexpected way. Examine all the current "consumers" of the code path you are proposing to change and consider whether the performance of any of them may be negatively impacted by the proposed change. List all these consequences as possible drawbacks.
        Since we did not perform a lookup from the concurrent
        `UUID-Set<Address>` and its reverse map in the hot paths, we didn't
        expect any performance degradation with this change. We perform a simple
        benchmark on this change and don't see any performance difference with
        the version before the change. See the benchmark for it:
        https://hazelcast.atlassian.net/wiki/spaces/PERF/pages/3949068293/Performance+Tests+for+5.1+Hostname+Fix
     

- Stability questions:
    - Can this new functionality affect the stability of a node or the entire cluster? How does the behavior of a node or a cluster degrade if there is an error in the implementation?
    - Can the new functionality be disabled? Can a user opt out? How?
    - Can the new functionality affect clusters which are not explicitly using it?
    - What testing and safe guards are being put in place to protect against unexpected problems?

### Testing Criteria

#### 
Unit and integration tests should:
- Verify that hostnames in TCP-IP member configuration work;
- Verify that hostnames in TCP-IP client configuration work;
- Verify that hostnames in WAN target configuration work;
- Verify that a hostname available only after the member starts works when used for establishing connections from other members;
- Verify that it's possible to use multiple hostnames to reference one member;
- Verify UUID-Address management with Persistence enabled remains working (consider restarting with same UUID);
- Verify TLS remains working (when host validation is enabled);
- Verify the performance doesn't significantly drop in different environments (On premise, Kubernetes, GKE, AWS deployments etc.)
- Verify cluster is correctly being formed, Persistence and WAN is working when `setPublicAdress` is applied
- Verify that the client from external network can connect to the cluster

It would definitely be better to test these scenarios also with hazelcast test containers as well.

#### Stress test

Stress tests must validate the Hazelcast work when:

- A member in the cluster gracefully shutdown
- A member forcefully shutdown
- Split-brain happens and resolves
- Cluster-wide crash happens on some cluster B when WAN replication is set between cluster A and B

The above scenarios must also be tested when Hazelcast persistence is
enabled.

#### Cloud tests
This fix must be tested on different cloud environments since they are
capable of flexible networking setups (they can have networking
structures that we do not easily encounter in other premise setups.).
e.g. expose externally feature of operator. We should definitely verify
that the hostnames work in these cloud networking setups. If possible,
automated tests should be implemented for these.
