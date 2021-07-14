---
title: Running Enterprise With Docker
description: How to use Jet Enterprise features in a Docker environment
id: version-4.2-docker-enterprise
original_id: docker-enterprise
---

This section provides coverage of usage of Hazelcast Jet Enterprise
features inside a Docker environment. For basic Docker usage you should
read the [Running With Docker](../operations/docker.md) first.

## Start Hazelcast Jet Enterprise Member

If you have your license key you can start a Hazelcast Jet Enterprise
member by running the following command:

```bash
docker run -e JET_LICENSE_KEY=<your-license-key> hazelcast/hazelcast-jet-enterprise
```

Among other output you should see lines similar to the following in the
log:

```text
2020-06-18 13:09:28,833 [ INFO] [main] [c.h.system]: Hazelcast Jet Enterprise 4.1 (20200429 - 8712224) starting at [172.17.0.5]:5701
...
2020-06-18 13:09:28,834 [ INFO] [main] [c.h.i.i.NodeExtension]: Checking Hazelcast Enterprise license...
2020-06-18 13:09:28,840 [ INFO] [main] [c.h.i.i.NodeExtension]: License{ ..... }
...
```
