package com.hazelcast.nio;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
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
