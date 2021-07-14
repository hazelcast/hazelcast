---
title: Introduction
description: Introduction to Hazelcast Jet Enterprise Edition
id: version-4.4-index
original_id: index
---

Hazelcast Jet Enterprise Edition is a commercial offering from Hazelcast
built on top of the open source product, and offers some additional
features such as:

* Management Center which allows for job monitoring and lifecycle
  management through a web-based UI.
* Additional security options such as TLS support
* Lossless cluster restart, which enables to shutdown and restart the
  cluster gracefully without losing the computational state.
* Job updates, which allows updating of a streaming job's pipeline from
  one version to the next without losing computational state.
* Blue/Green client support, which allows clients to switch from one
  cluster to the other when one of the clusters are down.
* Off-heap memory, which allows to large and efficient data storage
  inside `IMap` through the use of off-heap data structures.

Besides these, Jet Enterprise edition offers all the features of the
open-source version.
