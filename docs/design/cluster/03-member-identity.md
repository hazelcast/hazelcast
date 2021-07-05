# Identifying Cluster Members

|ℹ️ Since: 5.0| 
|-------------|

## Background

Cluster members and clients, when connected via TCP/IP to the cluster,
have been identified in past versions via network addresses. These
addresses are an internal implementation formed from a `host` string
(IP or actual hostname) and a numeric `port`.

There have been attempts to transition this approach to a `UUID` based
one, but that's incomplete; addresses are still heavily relied upon.

Using addresses for identification purposes presents us with multiple
problems:

* a members address can be specified either by IP or by hostname and the
  two will be treated as two completely different addresses, thus
  creating the illusion of two different members

* a member can have multiple network interfaces, thus multiple addresses
  associated with it; these will again be treated as completely
  different addresses

* members can have multiple connections between each other, each having
  different port numbers; such connections will again be treated as
  completely different addresses and members

Fully transitioning to the UUID-based approach or solving the problems
presented by network addresses are very costly undertakings. Recently we
have undertaken some more restricted fixes, which should ameliorate many
of the problems. That is what this document describes.

## Approach

The approach taken with the fixes is to try to keep track of address
groups belonging to the same member. We call such addresses
`aliases` (of each other).

Early steps of the cluster join process (member establishes a connection
to other members, starts handshake process) examine network addresses,
try to resolve them (find addresses of hostnames, for example)
and store what they can figure out as an alias group.

Later steps of the handshaking process (and of cluster membership in
general) add network addresses received from remote members to the alias
groups (for example, member view keys, addresses describing how remote
members see themselves or others).

Once the handshaking process finishes and a member becomes an
"integrated" member of a cluster, it will assume that it is now aware of
all the aliases of other member's addresses and will just use the
knowledge it has (alias group - member mappings).

## Difficulties

The approach chosen works in checked problem scenarios, but it's
very `brittle`. When network addresses can be matched up as aliases
depends very much on the timing of various operations, so breaking the
functionality is very simple to do. Extensive automated test coverage
should be developed to make sure that the fixes remain functional.

One issue that complicates things further is that when a member joins a
cluster, depending on the timing `multiple connections` can become
established between it and some other member (sometimes they both
connect to the other one simultaneously, and both connections stay there
and get used). Right now, the fixes handle this problem, but again, this
is yet another thing that requires testing to keep in place.x

## Scope

The changes implemented have been considered and tested from the 
perspective of clusters with TCP/IP based join and WAN replication 
(which is basically almost identical to regular TCP/IP joins). 

One aspect that has not been specifically considered is the feature 
of "public addresses", as defined in the context of advanced network 
configs. Scenarios related to this feature should be included and 
checked if current fixes are enough for them. 

