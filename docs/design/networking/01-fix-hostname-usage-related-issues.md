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
|Related Jira|https://hazelcast.atlassian.net/browse/HZ-528|
|Related Github issues|https://github.com/hazelcast/hazelcast/issues/16651|
|Document Status / Completeness|DRAFT / IN PROGRESS / DESIGN REVIEW / DONE|
|Requirement owner|_Requirement owner_|
|Developer(s)|_Ufuk Yılmaz_|
|Quality Engineer|_Viacheslav Sytnyk_|
|Support Engineer|_Support Engineer_|
|Technical Reviewers|_Vassilis Bekiaris_ _Josef Cacek_|
|Simulator or Soak Test PR(s) |_Link to Simulator or Soak test PR_|

### Background

#### Description

Several problems can arise when using hostnames in Hazelcast's network
and WAN configurations. With this effort, we aim to provide a seamless
usage of hostnames in the Hazelcast configurations by fixing related
hostname usage issues. In the source of the problems, the members have
multiple addresses if the hostname used in the related configs, and we
don't manage them properly because significant part of the code base is
written assuming a member will only have one public address. There is
indeed one public address, but it is not easy to understand that
different representations of this address are the same address. For
example, if multiple hostname/domain names refer to the same member, we
cannot understand that these two addresses refer to the same place
without doing hostname resolution and the parts of the code that use the
address is not enforced to perform this IP resolution while using it (it
is a bit complicated process if it's not managed in a single place).

What we propose as a solution to these issues is that we identify/refer
to other members and their belongings such as connections and other
stuffs based on member UUIDs instead of the network addresses. We will
try to manage these network addresses at Hazelcast networking level and
try not to use this `Address`es in the higher level as much as possible.

After replacing the Address usage with UUID in those high level places,
we need to implement a mechanism so that we can retrieve the addresses
of that member from the uuid, since we may need the address of the
member whose uuid we know in some places because we need these addresses
in the network layer operations (we can only use addresses in socket
bind/connect ops).

In order to resolve this address management, I plan to implement the
UUID-Set<Address> mapping and its inverse. Then, will try to replace all
high level Address usages with UUID.

#### Motivation
We want to provide seamless hostname usage experience to our users.
Until now, many of our users and customers have had problems in this
regard as shown in the known issues. Although the hostname usages can be
replaced with IP based addressing in simple networking setups, it is a
necessary functionality for maintaining complex network setups where IPs
are dynamically assigned to the machines that Hazelcast members will
work on. This is specifically important for cloud-based deployments that
could be the subject of more complex networking setups compared to on
premise deployments that may include members whose IP are assigned
dynamically and these addresses identified by DNS based hostname
resolution mechanism.

#### Goals
- Support hostname-based addressing in member-to-member communication
- Support hostname-based addressing in WAN configuration
- Support the hostname-based addressing in relevant discovery plugins
  (e.g. discovery in AWS, Azure, GCP, Kubernetes)

In summary, we want Hazelcast to work when using hostname based address
used instead of the IP address in any Hazelcast cluster configuration that
works when IP address is used as configuration element. In short, we
want any cluster setup that works properly when ip addresses are used,
to operate when hostnames are used.

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
to provide them a way to manage member addresses. So, for solving this
kind of problems, we should expose our managed `UUID-Set<Address>` and
its reverse mappings to our users. Franta pointed out the jet connectors
as a users of Hazelcast member APIs uses some `Address` dependent logic
inside. e.g. In Elasticsearch connector, we use member addresses to
check if in case the Hazelcast cluster runs on the same machines as
Elastic it uses the addresses to plan the job in a way the reading from
the local shard that belongs to same machine and doesn't go over the
network. This is also same for the hadoop connectors. This logic may go
wrong when Hazelcast members have multiple addresses.

Up until now, our users mainly use picked public address of the members
for member identification purposes in the member logs. We should not
make any big change in the member logs so as not to confuse our users.
We should store the address configured by the user as the primary
address and continue to print it in the member logs. Even if we do the
member identification with a UUID-based logic, we should keep the member
address in related places in order to show it in the logs.

- Which new concepts have been introduced to end-users? Can you provide examples for each?
  *TODO*: Need to provide examples


### Technical Design

To manage the address aliases, we will create `UUID -> Set<Address>` and
`Address'es -> UUID` mappings inside `TcpServerConnectionManager`. Since
we can access the UUIDs of the members after getting MemberHandshake
response, the entries of these maps will be created on connection
registration which is performed in the handshake processing. We will
remove these entries when the connections are terminated. In this
address removal, hot restart with same UUID case must be considered. If
we try to extend the lifetime of these map entries a little longer, this
may cause problems with Hot Restart with the same UUID. Since we cannot
manage the connections which are in the in progress state with a UUID
based logic, we need to manage these addresses only with address aliases
sets. I plan to use Zoltan's linked address implementation for it. See:
https://github.com/hazelcast/hazelcast/pull/19684

While trying to create this design, I tried to purify the methods of
TcpServerConnectionManager from `Address` in order to prevent the use of
`Address` at the upper levels. But, we had a strict dependency to
Address usage in socket bind and connect operations by nature and some
operation must trigger these bind/connect ops.   
For the most of the operations we can have already established
connection but for the join, wan, and some cluster (heartbeat)
operations, we depend on `connect` semantics. But even the fact that few
of the operations mentioned above dependent on connect semantics
prevents us from easily removing the Address usage from our operation
invocation services.

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

<!--
TODO: Answer the questions
- Questions about the change:
    - How does this work in a on-prem deployment? How about on AWS and Kubernetes?
    - How does the change behave in mixed-version deployments? During a version upgrade? Which migrations are needed?
    - What are the possible interactions with other features or sub-systems inside Hazelcast? How does the behavior of other code change implicitly as a result of the changes outlined in the design document? (Provide examples if relevant.)
    - Is there other ongoing or recent work that is related? (Cross-reference the relevant design documents.)
    - What are the edge cases? What are example uses or inputs that we think are uncommon but are still possible and thus need to be handled? How are these edge cases handled? Provide examples.
    - What are the effect of possible mistakes by other Hazelcast team members trying to use the feature in their own code? How does the change impact how they will troubleshoot things?
    - Mention alternatives, risks and assumptions. Why is this design the best in the space of possible designs? What other designs have been considered and what is the rationale for not choosing them?
    - Add links to any similar functionalities by other vendors, similarities and differentiators

- Questions about performance:
    - Does the change impact performance? How?
    - How is resource usage affected for “large” loads? For example, what do we expect to happen when there are 100000 items/entries? 100000 data structures? 1000000 concurrent operations?
    - Also investigate the consequences of the proposed change on performance. Pay especially attention to the risk that introducing a possible performance improvement in one area can slow down another area in an unexpected way. Examine all the current "consumers" of the code path you are proposing to change and consider whether the performance of any of them may be negatively impacted by the proposed change. List all these consequences as possible drawbacks.

- Stability questions:
    - Can this new functionality affect the stability of a node or the entire cluster? How does the behavior of a node or a cluster degrade if there is an error in the implementation?
    - Can the new functionality be disabled? Can a user opt out? How?
    - Can the new functionality affect clusters which are not explicitly using it?
    - What testing and safe guards are being put in place to protect against unexpected problems?

- Security questions:
    - Does the change concern authentication or authorization logic? If so, mention this explicitly tag the relevant security-minded reviewer as reviewer to the design document.
    - Does the change create a new way to communicate data over the network?  What rules are in place to ensure that this cannot be used by a malicious user to extract confidential data?
    - Is there telemetry or crash reporting? What mechanisms are used to ensure no sensitive data is accidentally exposed?

- Observability and usage questions:
    - Is the change affecting asynchronous / background subsystems?
        - If so, how can users and our team observe the run-time state via tracing?
        - Which other inspection APIs exist?
          (In general, think about how your coworkers and users will gain access to the internals of the change after it has happened to either gain understanding during execution or troubleshoot problems.)

    - Are there new APIs, or API changes (either internal or external)?
        - How would you document the new APIs? Include example usage.
        - What are the other components or teams that need to know about the new APIs and changes?
        - Which principles did you apply to ensure the APIs are consistent with other related features / APIs? (Cross-reference other APIs that are similar or related, for comparison.)

    - Is the change visible to users of Hazelcast or operators who run Hazelcast clusters?
        - Are there any user experience (UX) changes needed as a result of this change?
        - Are the UX changes necessary or clearly beneficial? (Cross-reference the motivation section.)
        - Which principles did you apply to ensure the user experience (UX) is consistent with other related features? (Cross-reference other features that have related UX, for comparison.)
        - Which other engineers or teams have you polled for input on the proposed UX changes? Which engineers or team may have relevant experience to provide feedback on UX?
    - Is usage of the new feature observable in telemetry? If so, mention where in the code telemetry counters or metrics would be added.

The section should return to the user stories in the motivations section, and explain more fully how the detailed proposal makes those stories work.

-->

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

It would definitely be better to test these scenarios also with hazelcast test containers as well.

#### Stress test
In the case where Hazelcast persistence is enabled, a new member can be
started to replace a crashed member. And, it can use the same UUID as
the crashing member but different network addresses. In this case, we
must manage the lifecycle of our UUID-Set<Address> mapping properly so
that there are no stale entries in it. This hot restart with same uuid
but different addresses case should be created repeatedly and tested to
see if there is any mistake.

#### Cloud tests
This fix must be tested on different cloud environments since they are
capable of flexible networking setups (they can have networking
structures that we do not easily encounter in other premise setups.).
e.g. expose externally feature of operator. We should definitely verify
that the hostnames work in these cloud networking setups. If possible,
automated tests should be implemented for these.
