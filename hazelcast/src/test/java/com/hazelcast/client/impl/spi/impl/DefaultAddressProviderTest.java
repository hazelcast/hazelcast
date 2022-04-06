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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultAddressProviderTest {

    @Test
    public void whenNoAddresses() throws UnknownHostException {
        ClientNetworkConfig config = new ClientNetworkConfig();
        DefaultAddressProvider provider = new DefaultAddressProvider(config, () -> false);
        Addresses addresses = provider.loadAddresses();

        assertPrimary(addresses, new Address("127.0.0.1", 5701));
        assertSecondary(addresses, new Address("127.0.0.1", 5702), new Address("127.0.0.1", 5703));
    }

    @Test
    public void whenExplicitNoPortAddress() throws UnknownHostException {
        ClientNetworkConfig config = new ClientNetworkConfig();
        config.addAddress("10.0.0.1");
        DefaultAddressProvider provider = new DefaultAddressProvider(config, () -> false);
        Addresses addresses = provider.loadAddresses();

        assertPrimary(addresses, new Address("10.0.0.1", 5701));
        assertSecondary(addresses, new Address("10.0.0.1", 5702), new Address("10.0.0.1", 5703));
    }

    @Test
    public void whenExplicitPorts() throws UnknownHostException {
        ClientNetworkConfig config = new ClientNetworkConfig();
        config.addAddress("10.0.0.1:5703");
        config.addAddress("10.0.0.1:5702");
        DefaultAddressProvider provider = new DefaultAddressProvider(config, () -> false);
        Addresses addresses = provider.loadAddresses();

        assertPrimary(addresses, new Address("10.0.0.1", 5703), new Address("10.0.0.1", 5702));
        assertSecondaryEmpty(addresses);
    }

    @Test
    public void whenMix() throws UnknownHostException {
        ClientNetworkConfig config = new ClientNetworkConfig();
        config.addAddress("10.0.0.1:5701");
        config.addAddress("10.0.0.1:5702");
        config.addAddress("10.0.0.2");
        DefaultAddressProvider provider = new DefaultAddressProvider(config, () -> false);
        Addresses addresses = provider.loadAddresses();

        assertPrimary(addresses, new Address("10.0.0.1", 5701),
                new Address("10.0.0.1", 5702),
                new Address("10.0.0.2", 5701));
        assertSecondary(addresses, new Address("10.0.0.2", 5702), new Address("10.0.0.2", 5703));
    }

    @Test
    public void whenBogusAddress() {
        ClientNetworkConfig config = new ClientNetworkConfig();
        config.addAddress(UUID.randomUUID().toString());
        DefaultAddressProvider provider = new DefaultAddressProvider(config, () -> false);
        Addresses addresses = provider.loadAddresses();

        assertPrimaryEmpty(addresses);
        assertSecondaryEmpty(addresses);
    }

    public void assertPrimary(Addresses addresses, Address... expected) {
        assertEquals(Arrays.asList(expected), addresses.primary());
    }

    public void assertSecondaryEmpty(Addresses addresses) {
        assertTrue(addresses.secondary().isEmpty());
    }

    public void assertPrimaryEmpty(Addresses addresses) {
        assertTrue(addresses.primary().isEmpty());
    }

    public void assertSecondary(Addresses addresses, Address... expected) {
        assertEquals(Arrays.asList(expected), addresses.secondary());
    }
}
