/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RemoteAddressProviderTest {

    private Map<Address, Address> expectedAddresses = new ConcurrentHashMap<Address, Address>();

    @Before
    public void setUp() throws UnknownHostException {
        expectedAddresses.put(new Address("10.0.0.1", 5701), new Address("198.51.100.1", 5701));
        expectedAddresses.put(new Address("10.0.0.1", 5702), new Address("198.51.100.1", 5702));
        expectedAddresses.put(new Address("10.0.0.2", 5701), new Address("198.51.100.2", 5701));
    }

    @Test
    public void testLoadAddresses() throws Exception {
        RemoteAddressProvider provider = new RemoteAddressProvider(() -> expectedAddresses, true);
        Collection<Address> addresses = provider.loadAddresses().primary();

        assertEquals(3, addresses.size());
        for (Address address : expectedAddresses.keySet()) {
            addresses.remove(address);
        }
        assertTrue(addresses.isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadAddresses_whenExceptionIsThrown() throws Exception {
        RemoteAddressProvider provider = new RemoteAddressProvider(() -> {
            throw new IllegalStateException("Expected exception");
        }, true);

        provider.loadAddresses();
    }

    @Test
    public void testTranslate_whenAddressIsNull_thenReturnNull() throws Exception {
        RemoteAddressProvider provider = new RemoteAddressProvider(() -> expectedAddresses, true);
        Address actual = provider.translate((Address) null);
        assertNull(actual);
    }

    @Test
    public void testTranslate() throws Exception {
        Address privateAddress = new Address("10.0.0.1", 5701);
        Address publicAddress = new Address("198.51.100.1", 5701);
        RemoteAddressProvider provider = new RemoteAddressProvider(() -> Collections.singletonMap(privateAddress, publicAddress),
                true);
        Address actual = provider.translate(privateAddress);

        assertEquals(publicAddress.getHost(), actual.getHost());
        assertEquals(publicAddress.getPort(), actual.getPort());
    }

    @Test
    public void testTranslate_dontUsePublic() throws Exception {
        Address privateAddress = new Address("10.0.0.1", 5701);
        Address publicAddress = new Address("198.51.100.1", 5701);
        RemoteAddressProvider provider = new RemoteAddressProvider(() -> Collections.singletonMap(privateAddress, publicAddress)
                , false);
        Address actual = provider.translate(privateAddress);

        assertEquals(privateAddress.getHost(), actual.getHost());
        assertEquals(privateAddress.getPort(), actual.getPort());
    }

    @Test
    public void testTranslate_whenNotFound_thenReturnNull() throws Exception {
        RemoteAddressProvider provider = new RemoteAddressProvider(() -> expectedAddresses, true);
        Address notAvailableAddress = new Address("127.0.0.3", 5701);
        Address actual = provider.translate(notAvailableAddress);

        assertNull(actual);
    }
}

