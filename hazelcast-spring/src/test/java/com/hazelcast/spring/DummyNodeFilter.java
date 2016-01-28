package com.hazelcast.spring;

import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.NodeFilter;

public class DummyNodeFilter implements NodeFilter {
    @Override
    public boolean test(DiscoveryNode candidate) {
        return false;
    }
}
