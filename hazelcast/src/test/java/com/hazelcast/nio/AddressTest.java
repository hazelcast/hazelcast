package com.hazelcast.nio;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class AddressTest {

    @Test(expected = IllegalArgumentException.class)
    public void newAddress_InetSocketAddress_whenHostUnresolved() throws UnknownHostException {
        InetSocketAddress inetAddress = InetSocketAddress.createUnresolved("dontexist", 1);
        new Address(inetAddress);
    }

    @Test(expected = NullPointerException.class)
    public void newAddress_InetSocketAddress_whenNull() throws UnknownHostException {
        new Address((InetSocketAddress)null);
    }
}
