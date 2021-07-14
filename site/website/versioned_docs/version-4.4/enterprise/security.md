---
title: Security
description: Configuring Security for Hazelcast Jet Enterprise
id: version-4.4-security
original_id: security
---

It is possible to configure Jet Enterprise to use TLS for all member to
member and member to client communications. Once TLS is enabled on a
member, all other members in the same cluster and all clients connecting
to the cluster must also have TLS enabled.

To configure TLS, you need a server certificate and a matching private
key and do the necessary configuration on the member side.

## Enabling TLS

To enable TLS on the member, you need to make the following changes
in `config/hazelcast.yaml`:

```yaml
hazelcast:
  network:
    ssl:
      enabled: true
      factory-class-name: com.hazelcast.nio.ssl.BasicSSLContextFactory
      properties:
        protocol: TLS
        keyStore: <path to keystore>
        keyStorePassword: <keystore password>
        keyStoreType: JKS
```

And a similar config needs to be set for the client:

```yaml
hazelcast-client:
  network:
    ssl:
      enabled: true
      factory-class-name: com.hazelcast.nio.ssl.BasicSSLContextFactory
      properties:
        protocol: TLS

        trustStore: <path to trust store>
        trustStorePassword: <path to trust store>
        trustStoreType: JKS
```

## Mutual Authentication

When a client connects to a node, the node can authenticate the client
by using mutual authentication. In this mode, the client also has to
present a certificate to the member upon connection. This requires some
additional configuration.

On the node, these additional properties must be set under the `ssl`
config option:

```yaml
# Following properties are only needed when the mutual authentication is used.
mutualAuthentication: REQUIRED
trustStore: <path to trust store>
trustStorePassword: changeit
trustStoreType: JKS
```

On the client, these additional properties must be set under the `ssl`
config option:

```yaml
# Following properties are only needed when the mutual authentication is used.
keyStore: /opt/hazelcast-client.keystore
keyStorePassword: keystorePassword123
keyStoreType: JKS
mutualAuthentication: REQUIRED
```

## TLS Impact on Performance

TLS can have some effect on performance when running jobs, as all node
to node communications need to be encrypted. During an aggregation or
another partitioned operation in a job, large amounts of data might be
transferred from one node to another.

Jet makes use of several techniques that reduce the need for data to be
transferred across the nodes, however this is sometimes unavoidable.
When using Java SSL, the performance impact can be between 5-30%
depending on how much network is utilized. When using OpenSSL, we
measured the impact to be minimal regardless of the amount of network
traffic. Detailed benchmarks are available upon request.

## OpenSSL Configuration

By default Jet uses the Java implementation of SSL. For better
performance, it is also possible to use Jet with OpenSSL instead. For
configuring OpenSSL we recommend going through the guide in
[Hazelcast 4.1.1 Reference Manual](https://docs.hazelcast.org/docs/4.1.1/manual/html-single/index.html#integrating-openssl-boringssl).

## Remote Hazelcast Sources and Sinks

You can configure TLS for remote [Hazelcast sources and sinks](../api/sources-sinks#imap)
by passing a ClientConfig with TLS enabled. Jet will create a new
client for the remote cluster, and will use the connection options
configured in the config.

```java
ClientConfig config = new ClientConfig();
SSLConfig sslConfig = new SSLConfig()
    .setEnabled(true)
    .setFactoryClassName(..)
    .setProperties(..);
config.getNetworkConfig().setSSLConfig(sslConfig);

Pipeline p = Pipeline.create();
p.readFrom(Sources.map("map_name", config))
 ...
```
