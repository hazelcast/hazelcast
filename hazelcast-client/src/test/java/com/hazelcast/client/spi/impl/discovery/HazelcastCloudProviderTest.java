/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.logging.Logger.getLogger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HazelcastCloudProviderTest {

    private Map<Address, Address> expectedAddresses = new ConcurrentHashMap<Address, Address>();
    private HazelcastCloudDiscovery hazelcastCloudDiscovery;
    private HazelcastCloudAddressProvider provider;

    @Before
    public void setUp() throws UnknownHostException {
        expectedAddresses.put(new Address("10.0.0.1", 5701), new Address("198.51.100.1", 5701));
        expectedAddresses.put(new Address("10.0.0.1", 5702), new Address("198.51.100.1", 5702));
        expectedAddresses.put(new Address("10.0.0.2", 5701), new Address("198.51.100.2", 5701));

        hazelcastCloudDiscovery = mock(HazelcastCloudDiscovery.class);
        when(hazelcastCloudDiscovery.discoverNodes()).thenReturn(expectedAddresses);

        LoggingService loggingService = mock(LoggingService.class);
        when(loggingService.getLogger(eq(HazelcastCloudAddressProvider.class))).thenReturn(getLogger(HazelcastCloudProviderTest.class));

        provider = new HazelcastCloudAddressProvider(hazelcastCloudDiscovery, loggingService);
    }

    @Test
    public void testLoadAddresses() {
        Collection<Address> addresses = provider.loadAddresses().primary();

        assertEquals(3, addresses.size());
        for (Address address : expectedAddresses.keySet()) {
            addresses.remove(address);
        }
        assertTrue(addresses.isEmpty());
    }

    @Test
    public void testLoadAddresses_whenExceptionIsThrown() {
        when(hazelcastCloudDiscovery.discoverNodes()).thenThrow(new IllegalStateException("Expected exception"));

        Collection<Address> addresses = provider.loadAddresses().primary();

        assertEquals("Expected that no addresses are loaded", 0, addresses.size());
    }

}

