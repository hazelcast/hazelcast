package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.util.AddressHelper;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Default address provider of hazelcast
 * Loads addresses from hazelcast configuration
 */
public class DefaultAddressProvider implements AddressProvider {

    private ClientNetworkConfig networkConfig;

    public DefaultAddressProvider(ClientNetworkConfig networkConfig) {
        this.networkConfig = networkConfig;

    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        final List<String> addresses = networkConfig.getAddresses();
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        Collections.shuffle(addresses);
        for (String address : addresses) {
            socketAddresses.addAll(AddressHelper.getSocketAddresses(address));
        }
        return socketAddresses;
    }
}
