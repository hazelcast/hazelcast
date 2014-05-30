package com.hazelcast.client.connection;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Used to load addresses from third-party services like aws.
 */
public interface ServiceAddressLoader {

    /**
     * @return Collection of InetSocketAddress
     */
    public Collection<InetSocketAddress> loadAddresses();

    /**
     * Clear internals.
     */
    public void clear();
}
