package com.hazelcast.client.connection;

import com.hazelcast.nio.Address;

/**
 *  Address Translator is used for resolve private ip
 *  addresses of cloud services.
 */
public interface AddressTranslator {

    /**
     * Translates the given address to another address specific to
     * network or service
     *
     * @param address
     * @return new address if given address is known, otherwise return null
     */
    public Address translate(Address address);

    /**
     * Refreshes the internal lookup table if necessary.
     */
    public void refresh();

    /**
     * Shutdown method to clear internals
     */
    public void shutdown();
}
