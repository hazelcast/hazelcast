/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientUtil;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.management.ClientConnectionProcessListenerRegistry;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTranslateTest extends ClientTestSupport {
    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

    private Address privateAddress;
    private Address publicAddress;

    @Before
    public void setup() throws Exception {
        Hazelcast.newHazelcastInstance();

        // correct private address
        privateAddress = new Address("127.0.0.1", 5701);
        // incorrect public address
        publicAddress = new Address("192.168.0.1", 5701);
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(expected = Exception.class)
    public void testTranslateIsUsed() {
        // given
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        HazelcastInstance client = HazelcastClientUtil.newHazelcastClient(config, new TestAddressProvider(true));
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHazelcastClientInstanceImpl(client));

        // when
        clientConnectionManager.start();

        // then
        // throws exception because it can't connect to the cluster using translated public unreachable address
    }

    @Test
    public void testTranslateIsNotUsedOnGettingExistingConnection() {
        // given
        TestAddressProvider provider = new TestAddressProvider(false);
        HazelcastInstance client = HazelcastClientUtil.newHazelcastClient(new ClientConfig(), provider);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHazelcastClientInstanceImpl(client));

        clientConnectionManager.start();
        clientConnectionManager.reset();

        clientConnectionManager.getOrConnectToAddress(privateAddress, false);
        provider.shouldTranslate = true;

        // when
        Connection connection = clientConnectionManager.getOrConnectToAddress(privateAddress, false);

        // then
        assertNotNull(connection);
    }

    @Test
    public void testTranslateIsUsedWhenMemberHasPublicClientAddress() throws UnknownHostException {
        // given
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "true");

        HazelcastInstance client = HazelcastClientUtil.newHazelcastClient(clientConfig, null);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHazelcastClientInstanceImpl(client));
        clientConnectionManager.start();

        // private member address is unreachable
        Member member = new MemberImpl(new Address("192.168.0.1", 5701), VERSION, false, UUID.randomUUID());
        // public member address is reachable
        member.getAddressMap().put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"),
                new Address("127.0.0.1", 5701));

        // when
        Connection connection = clientConnectionManager.getOrConnectToMember(member, false);

        // then
        assertNotNull(connection);
    }

    @Test(expected = Exception.class)
    public void testTranslateIsNotUsedWhenPublicIpDisabled() throws UnknownHostException {
        // given
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "false");

        HazelcastInstance client = HazelcastClientUtil.newHazelcastClient(clientConfig, null);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHazelcastClientInstanceImpl(client));
        clientConnectionManager.start();

        // private member address is incorrect
        Member member = new MemberImpl(new Address("192.168.0.1", 5701), VERSION, false);
        // public member address is correct
        member.getAddressMap().put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"), new Address("127.0.0.1", 5701));

        // when
        clientConnectionManager.getOrConnectToMember(member, false);

        // then
        // throws exception because it can't connect to the incorrect member address
    }

    @Test(expected = Exception.class)
    public void testTranslateFromMemberIsNotUsedWhenAlreadyTranslatedByAddressProvider() throws UnknownHostException {
        // given
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "true");

        TestAddressProvider provider = new TestAddressProvider(false);
        HazelcastInstance client = HazelcastClientUtil.newHazelcastClient(clientConfig, provider);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHazelcastClientInstanceImpl(client));
        clientConnectionManager.start();
        provider.shouldTranslate = true;
        privateAddress = new Address("192.168.0.1", 5702);

        // private member address is correct
        Member member = new MemberImpl(privateAddress, VERSION, false, UUID.randomUUID());
        // public member address is correct
        member.getAddressMap().put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"),
                new Address("127.0.0.1", 5701));

        // when
        clientConnectionManager.getOrConnectToMember(member, false);

        // then
        // throws exception because it can't connect to the incorrect address
    }

    private class TestAddressProvider implements AddressProvider {

        private boolean shouldTranslate;

        private TestAddressProvider(boolean shouldTranslate) {
            this.shouldTranslate = shouldTranslate;
        }

        @Override
        public Address translate(Address address) {
            if (!shouldTranslate) {
                return address;
            }

            if (address.equals(privateAddress)) {
                return publicAddress;
            }
            return null;
        }

        @Override
        public Address translate(Member member) {
            return member.getAddress();
        }

        @Override
        public Addresses loadAddresses(ClientConnectionProcessListenerRegistry listenerRunner) {
            try {
                return new Addresses(List.of(new Address("127.0.0.1", 5701)));
            } catch (UnknownHostException e) {
                return null;
            }
        }
    }
}
