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
    Address translate(Address address);

    /**
     * Refreshes the internal lookup table if necessary.
     */
    void refresh();

}
