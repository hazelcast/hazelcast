/**
 * 
 */
package com.hazelcast.impl.base;

import java.io.Serializable;

import com.hazelcast.nio.Address;

public class AddressAwareException implements Serializable {
    private Exception exception;
    private Address address; //where the exception happened

    public AddressAwareException(Exception exception, Address address) {
        this.exception = exception;
        this.address = address;
    }

    public AddressAwareException() {
    }

    public Exception getException() {
        return exception;
    }

    public Address getAddress() {
        return address;
    }
}