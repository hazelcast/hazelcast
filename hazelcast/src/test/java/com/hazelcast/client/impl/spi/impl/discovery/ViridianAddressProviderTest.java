/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.management.ClientConnectionProcessListenerRunner;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ViridianAddressProviderTest {

    private static final Address PRIVATE_MEMBER_ADDRESS = Address.createUnresolvedAddress("10.0.0.1", 30000);
    private static final Address PUBLIC_MEMBER_ADDRESS = Address.createUnresolvedAddress("34.215.70.2", 32661);

    private static final Address PRIVATE_TPC_ADDRESS = Address.createUnresolvedAddress("10.0.0.1", 42000);
    private static final Address PUBLIC_TPC_ADDRESS = Address.createUnresolvedAddress("34.215.70.2", 52553);
    private static final Address NON_EXISTENT_PRIVATE_ADDRESS = Address.createUnresolvedAddress("1.1.1.1", 1111);

    private final Map<Address, Address> privateToPublic = new HashMap<>();
    private final List<Address> members = new ArrayList<>();

    @Test
    public void testLoadAddresses() throws Exception {
        setUp();
        ViridianAddressProvider provider = new ViridianAddressProvider(createDiscovery());

        ClientConnectionProcessListenerRunner listener = createListenerRunner();
        Addresses addresses = provider.loadAddresses(listener);
        Collection<Address> primaries = addresses.primary();
        Collection<Address> secondaries = addresses.secondary();

        assertThat(primaries).containsExactly(PRIVATE_MEMBER_ADDRESS);
        assertThat(secondaries).isEmpty();
        verify(listener, times(1)).onPossibleAddressesCollected(Collections.singletonList(PRIVATE_MEMBER_ADDRESS));
    }

    @Test
    public void testLoadAddresses_withTpc() throws Exception {
        setUpWithTpc();
        ViridianAddressProvider provider = new ViridianAddressProvider(createDiscovery());
        ClientConnectionProcessListenerRunner listener = createListenerRunner();
        Addresses addresses = provider.loadAddresses(listener);
        Collection<Address> primaries = addresses.primary();
        Collection<Address> secondaries = addresses.secondary();

        assertThat(primaries).containsExactly(PRIVATE_MEMBER_ADDRESS);
        assertThat(secondaries).isEmpty();
        verify(listener, times(1)).onPossibleAddressesCollected(Collections.singletonList(PRIVATE_MEMBER_ADDRESS));    }

    @Test(expected = IllegalStateException.class)
    public void testLoadAddresses_whenExceptionIsThrown() throws Exception {
        ViridianAddressProvider provider = new ViridianAddressProvider(createBrokenDiscovery());
        provider.loadAddresses(createListenerRunner());
    }

    @Test
    public void testTranslate_whenAddressIsNull() throws Exception {
        ViridianAddressProvider provider = new ViridianAddressProvider(createDiscovery());
        provider.loadAddresses(createListenerRunner());

        assertNull(provider.translate((Address) null));
    }

    @Test
    public void testTranslate() throws Exception {
        setUp();
        ViridianAddressProvider provider = new ViridianAddressProvider(createDiscovery());
        provider.loadAddresses(createListenerRunner());

        assertEquals(PUBLIC_MEMBER_ADDRESS, provider.translate(PRIVATE_MEMBER_ADDRESS));
        assertNull(provider.translate(PRIVATE_TPC_ADDRESS));
        assertNull(provider.translate(NON_EXISTENT_PRIVATE_ADDRESS));
    }

    @Test
    public void testTranslate_withTpc() throws Exception {
        setUpWithTpc();
        ViridianAddressProvider provider = new ViridianAddressProvider(createDiscovery());
        provider.loadAddresses(createListenerRunner());

        assertEquals(PUBLIC_MEMBER_ADDRESS, provider.translate(PRIVATE_MEMBER_ADDRESS));
        assertEquals(PUBLIC_TPC_ADDRESS, provider.translate(PRIVATE_TPC_ADDRESS));
        assertNull(provider.translate(NON_EXISTENT_PRIVATE_ADDRESS));
    }

    @Test
    public void testTranslate_whenAddressNotFoundInitially() throws Exception {
        setUp();
        ViridianAddressProvider provider = new ViridianAddressProvider(createDiscoveryWithEmptyInitialResponse());
        provider.loadAddresses(createListenerRunner());

        assertEquals(PUBLIC_MEMBER_ADDRESS, provider.translate(PRIVATE_MEMBER_ADDRESS));
        assertNull(provider.translate(PRIVATE_TPC_ADDRESS));
        assertNull(provider.translate(NON_EXISTENT_PRIVATE_ADDRESS));
    }

    @Test
    public void testTranslate_withTpc_whenAddressNotFoundInitially() throws Exception {
        setUpWithTpc();
        ViridianAddressProvider provider = new ViridianAddressProvider(createDiscoveryWithEmptyInitialResponse());
        provider.loadAddresses(createListenerRunner());

        assertEquals(PUBLIC_MEMBER_ADDRESS, provider.translate(PRIVATE_MEMBER_ADDRESS));
        assertEquals(PUBLIC_TPC_ADDRESS, provider.translate(PRIVATE_TPC_ADDRESS));
        assertNull(provider.translate(NON_EXISTENT_PRIVATE_ADDRESS));
    }

    private void setUp() {
        privateToPublic.put(PRIVATE_MEMBER_ADDRESS, PUBLIC_MEMBER_ADDRESS);
        members.add(PRIVATE_MEMBER_ADDRESS);
    }

    private void setUpWithTpc() {
        setUp();

        // Also add the TPC Port
        privateToPublic.put(PRIVATE_TPC_ADDRESS, PUBLIC_TPC_ADDRESS);
    }

    private HazelcastCloudDiscovery createDiscovery() {
        HazelcastCloudDiscovery discovery = mock(HazelcastCloudDiscovery.class);

        HazelcastCloudDiscovery.DiscoveryResponse response = new HazelcastCloudDiscovery.DiscoveryResponse(
                new HashMap<>(privateToPublic),
                new ArrayList<>(members)
        );

        when(discovery.discoverNodes()).thenReturn(response);
        return discovery;
    }

    private HazelcastCloudDiscovery createBrokenDiscovery() {
        HazelcastCloudDiscovery discovery = mock(HazelcastCloudDiscovery.class);
        when(discovery.discoverNodes()).thenThrow(new IllegalStateException("expected"));
        return discovery;
    }

    private HazelcastCloudDiscovery createDiscoveryWithEmptyInitialResponse() {
        HazelcastCloudDiscovery discovery = mock(HazelcastCloudDiscovery.class);

        HazelcastCloudDiscovery.DiscoveryResponse emptyResponse = new HazelcastCloudDiscovery.DiscoveryResponse(
                Collections.emptyMap(),
                Collections.emptyList()
        );

        HazelcastCloudDiscovery.DiscoveryResponse response = new HazelcastCloudDiscovery.DiscoveryResponse(
                new HashMap<>(privateToPublic),
                new ArrayList<>(members)
        );

        when(discovery.discoverNodes()).thenReturn(emptyResponse).thenReturn(response);
        return discovery;
    }

    private ClientConnectionProcessListenerRunner createListenerRunner() {
        return mock(ClientConnectionProcessListenerRunner.class);
    }
}
