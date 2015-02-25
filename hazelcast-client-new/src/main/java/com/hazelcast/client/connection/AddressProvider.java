package com.hazelcast.client.connection;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Provides initial addresses for client to find and connect to a node
 */
public interface AddressProvider {

    /**
     * @return Collection of InetSocketAddress
     */
    Collection<InetSocketAddress> loadAddresses();

}
