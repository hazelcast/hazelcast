---
title: Off-Heap Data Structures
description: Utilize off-heap memory in Jet
id: version-4.0-off-heap
original_id: off-heap
---

Jet runs inside a Java Virtual Machine which uses garbage collected heap
memory. Depending on heap size, version of the JVM and the specific
garbage collector used garbage collection can suddenly create large
pauses in the application, leading to unpredictable spikes in latency
and drops in throughput.

To avoid this, Jet supports storing data in `IMap` in what's called
_native_ or _off-heap_ memory using the [High-Density Memory](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#using-high-density-memory-store-with-map)
feature of Hazelcast. This allows JVM to operate with smaller heaps and
do garbage collection more efficiently, while allowing Jet to store
large amount of data.

Currently off-heap memory support in Jet is limited to data stored in
`IMap`, the state for running jobs are still in on-heap data structures.

## Configuring Off-Heap Memory

You can configure an `IMap` to use off-heap memory by configuring
`hazelcast.yaml`. First, you need to dedicate a specific portion of the
physical memory available to off-heap and then enable off-heap memory
for the specific maps. The node needs to have enough free physical
memory for this feature to work. For example, to dedicate `128GB` of
memory for off-heap and to enable it for the `IMap` with name
`huge-map`, you can make the following configuration changes in
`hazelcast.yaml`:

```yaml
hazelcast:
  native-memory:
    enabled: true
    size:
      unit: GIGABYTES
      value: 128
  map:
    huge-map:
      in-memory-format: NATIVE
```

For additional configuration options, see [configuring high-density memory](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#configuring-high-density-memory-store)
section of the Hazelcast manual.

## Persistent Memory

Off-heap memory can also make use of persistent memory technologies such
as [Intel Optane](https://www.intel.com/content/www/us/en/architecture-and-technology/optane-dc-persistent-memory.html).
Depending on the value size, this offers almost similar performance to
memory at a lower cost, making it attractive for storing large amounts
of data efficiently.

Please refer to the [configuring persistent memory](https://docs.hazelcast.org/docs/4.0/manual/html-single/index.html#using-persistent-memory)
section of the Hazelcast manual for details on how to enable persistent
memory.