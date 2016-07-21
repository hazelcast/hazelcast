package com.hazelcast.spi;

import com.hazelcast.nio.Address;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class LiveOperationsTest {

    private Address address;
    private Address anotherAddress;

    private LiveOperations liveOperations;

    @Before
    public void setUp() throws Exception {
        address = new Address("127.0.0.1", 5701);
        anotherAddress = new Address("127.0.0.1", 5702);

        liveOperations = new LiveOperations(address);
    }

    @Test
    public void testAdd() {
        liveOperations.add(anotherAddress, 23);

        Set<Address> addresses = liveOperations.addresses();
        assertEquals(1, addresses.size());
        assertEquals(anotherAddress, addresses.iterator().next());
    }

    @Test
    public void testAdd_whenAddressIsNull_thenAddsLocalAddress() {
        liveOperations.add(null, 42);

        Set<Address> addresses = liveOperations.addresses();
        assertEquals(1, addresses.size());
        assertEquals(address, addresses.iterator().next());
    }

    @Test
    public void testAdd_whenCallIdIsZero_thenNoAddressIsAdded() {
        liveOperations.add(anotherAddress, 0);

        Set<Address> addresses = liveOperations.addresses();
        assertEquals(0, addresses.size());
    }

    @Test
    public void testCallIds() {
        liveOperations.add(anotherAddress, 23);

        long[] callIds = liveOperations.callIds(anotherAddress);

        assertEquals(1, callIds.length);
        assertEquals(23, callIds[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCallIds_whenAddressIsUnknown_thenThrowException() {
        liveOperations.callIds(address);
    }

    @Test
    public void testClearAndInitMember() {
        liveOperations.add(address, 23);

        liveOperations.clear();

        liveOperations.initMember(anotherAddress);

        long[] callIds = liveOperations.callIds(anotherAddress);
        assertEquals(0, callIds.length);
    }
}
