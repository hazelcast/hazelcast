# TPC Engine and Client Interaction

## Exposed Ports

TPC engine exposes a port for each of its event loops. The number of event 
loops(by default, equals to the number of available processors) and the 
ranges of the ports(by default, it is between `11000` and `21000`) 
to be exposed are configurable.

Along with them, Hazelcast continues to expose the classic Hazelcast port 
(`5701`, by default).

## Client Interaction

Clients will continue to use the classic Hazelcast ports for the 
initial authentication.

If the TPC is enabled on the client-side, clients will make use of the two 
newly added codecs to authenticate with the cluster. (note: these codecs were removed later on and 
`tpcPorts` and `tpcToken` fields were added to the stable codecs. This was done as part of making Alto stable)

- `ExperimentalAuthenticationCodec`
  - Counterpart of `ClientAuthenticationCodec`
- `ExperimentalAuthenticationCustomCodec`
  - Counterpart of `ClientAuthenticationCustomCodec`

These experimental codecs exist as the TPC feature is in BETA, and we didn't
want to put experimental parameters to the stable codecs. Once the TPC is 
promoted to stable status, these experimental codecs will be merged
with the existing stable authentication codecs.

These experimental codecs contain all the request and response parameters of
their counterparts. On top of that, they contain the following two extra
response parameters:

- `tpcPorts`: List of the exposed TPC ports, or null if the TPC is disabled
on the server-side.
- `tpcToken`: The token to be used during the authentication of the TPC 
connections, or null if the TPC is disabled on the server-side.

The client uses the classic Hazelcast port to authenticate, as usual. The
same discovery mechanisms are also used to specify the addresses to connect,
and there are no TPC specific changes in that area.

When the TPC-enabled client connects to the classic Hazelcast port of the TPC-enabled
cluster with these new experimental codecs, on top of the regular authentication
procedure and response, these two extra fields are returned to the client. Only
then the client knows about the exposed TPC ports of the cluster member.

The ports are ordered list with the following sample format:

```
[11000, 11001, 11002, ...]
```

The client will use the hostname of the classic Hazelcast connection and these
ports to establish connections to each of the event loops.

The returned token is a 64-bytes long byte array, generated using 
[Secure Random](https://docs.oracle.com/javase/8/docs/api/java/security/SecureRandom.html).
A new token is generated for each authenticated client connection to any of the
cluster members. So, there is a separate token associated with each successful
client connection to the classic Hazelcast ports of the cluster members.

The connection to the TPC ports follows a similar route as the connections to
classic ports.

First, 3-bytes long protocol identifier is sent. (bytearray representation of `CP2`).

Then, the client authenticates to the TPC event loop using the new 
`ExperimentalTpcAuthenticationCodec`, which has the following definition. (note: this codec was later removed and 
replaced by the new `ClientTpcAuthenticationCodec` when the TPC was promoted to stable status.)

```yaml
- id: 3
  name: tpcAuthentication
  since: 2.6
  doc: |
    Makes an authentication request to TPC channels.
  request:
    retryable: true
    partitionIdentifier: -1
    params:
      - name: uuid
        type: UUID
        nullable: false
        since: 2.6
        doc: |
          UUID of the client.
      - name: token
        type: byteArray
        nullable: false
        since: 2.6
        doc: |
          Authentication token bytes for the TPC channels
  response: {}
```

In that authentication request, the client sends the necessary information
to identify itself (its UUID), and the TPC token associated with the
client connection to the classic Hazelcast port, to prove its identity.

If the token associated with the client UUID matches the token sent, the 
server returns a successful authentication response to the client (an empty message). 

The server has the following protections for unauthorized connection attempts:

- If anything other than `CP2` is sent as the protocol bytes initially, the 
connection is closed immediately.
- There is a maximum message length limit (configured with
`hazelcast.client.protocol.max.message.bytes` property with the default values of 
4096-bytes) for not-yet-authenticated connections. If the messages sent before 
the authentication exceeds that limit, the connection is closed immediately. 
The limit checks are removed after the authentication.
- If the first message sent over the connection is anything other than 
`ExperimentalTpcAuthenticationCodec`, the connection is closed immediately to avoid
attempts of unauthorized use.
- During authentication:
  - If no active client connection for the given UUID is found, the connection is
  closed immediately.
  - If there is an active client connection for that UUID, but it does not have a
  TPC token associated with it(i.e. it is a TPC-disabled client), the connection
  is closed immediately.
  - If there is an active client connection for that UUID, and it has a token 
  associated with it, but the token mismatches with the one sent in the 
  authentication request, the connection is closed immediately.

## TLS

The TPC engine does not support TLS yet. Therefore, the client uses plain-text
communication for TPC connections.

## References

- Full codec definitions: https://github.com/hazelcast/hazelcast-client-protocol/blob/master/protocol-definitions/Experimental.yaml
- TDDs:
  - https://hazelcast.atlassian.net/wiki/spaces/HZC/pages/4361355275/TPC+Aware+Client+TDD
  - https://hazelcast.atlassian.net/wiki/spaces/HZC/pages/4434919462/TPC+Aware+Client+Connection+Hardening
