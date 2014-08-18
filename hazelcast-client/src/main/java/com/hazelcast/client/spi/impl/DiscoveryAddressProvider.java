package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientDiscoveryConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;

/**
 * Multicast Discovery Address Provider. Sends out multicast discovery messages to locate applicable cluster.
 */
public class DiscoveryAddressProvider implements AddressProvider {
    private static final ILogger LOGGER = Logger.getLogger(DiscoveryAddressProvider.class);
    private final DiscoveryClient discoveryClient;
    private volatile Set<String> discovered;

    DiscoveryAddressProvider(HazelcastClient client, ClientConfig config) {
        discoveryClient = new DiscoveryClient(client, config);
    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        updateDiscoveredTable();
        final Set<String> discoveredTable = getDiscoveredTable();
        final Collection<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>(discoveredTable.size());

        for (String privateAddress : discoveredTable) {
            addresses.addAll(AddressHelper.getSocketAddresses(privateAddress));
        }
        return addresses;

    }

    private Set<String> getDiscoveredTable() {
        Set<String> table = discovered;
        return table != null ? table : Collections.<String>emptySet();
    }

    private void updateDiscoveredTable() {
        try {
            discovered = discoveryClient.getAddresses();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Discovered addresses failed to load : " + e.getMessage());
        }
    }
}
