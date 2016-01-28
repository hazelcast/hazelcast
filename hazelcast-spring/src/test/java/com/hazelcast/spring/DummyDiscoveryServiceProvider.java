package com.hazelcast.spring;

import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;

public class DummyDiscoveryServiceProvider implements DiscoveryServiceProvider {
    @Override
    public DiscoveryService newDiscoveryService(DiscoveryServiceSettings settings) {
        return null;
    }
}
