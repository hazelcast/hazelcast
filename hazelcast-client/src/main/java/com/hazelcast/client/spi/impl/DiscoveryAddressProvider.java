package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.logging.Level;

/**
 * Multicast Discovery Address Provider. Sends out multicast discovery messages to locate applicable cluster.
 */
public class DiscoveryAddressProvider implements AddressProvider {
    private static final ILogger LOGGER = Logger.getLogger(DiscoveryAddressProvider.class);
    private final DiscoveryClient discoveryClient;
    private volatile Set<InetSocketAddress> discovered;

    DiscoveryAddressProvider(HazelcastClient client, ClientConfig config) {
        discoveryClient = new DiscoveryClient(client, config);
    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        updateDiscoveredTable();
        final Set<InetSocketAddress> discoveredTable = getDiscoveredTable();
        final Collection<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>(discoveredTable.size());

        addresses.addAll(discoveredTable);

        return addresses;
    }

    private Set<InetSocketAddress> getDiscoveredTable() {
        Set<InetSocketAddress> table = discovered;
        return table != null ? table : Collections.<InetSocketAddress>emptySet();
    }

    private void updateDiscoveredTable() {
        try {
            discovered = discoveryClient.getAddresses();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Discovered addresses failed to load : " + e.getMessage());
        }
    }
}
