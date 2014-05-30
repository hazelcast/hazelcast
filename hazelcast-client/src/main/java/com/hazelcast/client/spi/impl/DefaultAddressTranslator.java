package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.nio.Address;

/**
 * Default Address Translator is a no-op. It always returns the given address.
 */
public class DefaultAddressTranslator implements AddressTranslator {

    @Override
    public Address translate(Address address) {
        return address;
    }

    @Override
    public void updateLookupTable() {

    }

    @Override
    public void shutdown() {

    }
}
