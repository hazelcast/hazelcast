package com.hazelcast.spring;

import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;

import java.util.Collections;
import java.util.Map;

public class DummyDiscoveryStrategy implements DiscoveryStrategy {
    @Override
    public void start() {

    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        return null;
    }

    @Override
    public void destroy() {

    }

    @Override
    public PartitionGroupStrategy getPartitionGroupStrategy() {
        return null;
    }

    @Override
    public Map<String, Object> discoverLocalMetadata() {
        return Collections.emptyMap();
    }
}
