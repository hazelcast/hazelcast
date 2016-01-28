package com.hazelcast.spring;

import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;

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
}
