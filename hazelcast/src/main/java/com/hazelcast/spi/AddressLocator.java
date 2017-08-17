package com.hazelcast.spi;

import com.hazelcast.config.AddressLocatorConfig;

import java.net.InetSocketAddress;

/**
 * Allow to customize:
 * 1. What address Hazelcast will bind to
 * 2. What address Hazelcast will advertise to others
 *
 * This is practical in some cloud environments where the default strategy is not yielding good results.
 *
 * See also {@link com.hazelcast.instance.DefaultAddressPicker}
 * and {@link com.hazelcast.config.NetworkConfig#setAddressLocatorConfig(AddressLocatorConfig)}
 *
 *
 */
public interface AddressLocator {
    /**
     * What address should Hazelcast bind to. When port is set to 0 then it will use a port as configured in Hazelcast
     * network configuration
     *
     * @return Address to bind to
     */
    InetSocketAddress getBindAddress();

    /**
     * What address should Hazelcast advertise to other members and clients.
     * When port is 0 then it will broadcast the same port it is bound to.
     *
     * @return Address to advertise to others
     */
    InetSocketAddress getPublicAddress();
}
