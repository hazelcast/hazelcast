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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.MemberAddressProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DelegatingAddressPickerTest {

    private Address memberBindAddress;
    private Address clientBindAddress;
    private Address textBindAddress;
    private Address wan1BindAddress;

    private Address memberPublicAddress;
    private Address clientPublicAddress;
    private Address textPublicAddress;
    private Address wan1PublicAddress;

    private ILogger logger;

    private DelegatingAddressPicker picker;

    private final EndpointQualifier wanEndpointQualifier = EndpointQualifier.resolve(ProtocolType.WAN, "wan1");


    @Before
    public void setup() throws Exception {
        memberBindAddress = new Address("127.0.0.1", 2000);
        clientBindAddress = new Address("127.0.0.1", 2001);
        textBindAddress = new Address("127.0.0.1", 2002);
        wan1BindAddress = new Address("127.0.0.1", 2003);
        logger = Logger.getLogger(DelegatingAddressPickerTest.class);

        memberPublicAddress = new Address("10.10.10.10", 3000);
        clientPublicAddress = new Address("10.10.10.10", 3001);
        textPublicAddress = new Address("10.10.10.10", 3002);
        wan1PublicAddress = new Address("10.10.10.10", 3003);
    }

    @After
    public void tearDown() throws Exception {
        for (ServerSocketChannel channel : picker.getServerSocketChannels().values()) {
            channel.close();
        }
    }

    @Test
    public void testPickAddress_fromAdvancedNetworkConfig() throws Exception {
        Config config = createAdvancedNetworkConfig();
        picker = new DelegatingAddressPicker(new AnAddressProvider(), config, logger);

        picker.pickAddress();

        assertEquals(memberBindAddress, picker.getBindAddress(EndpointQualifier.MEMBER));
        assertEquals(clientBindAddress, picker.getBindAddress(EndpointQualifier.CLIENT));
        assertEquals(textBindAddress, picker.getBindAddress(EndpointQualifier.REST));
        assertEquals(wan1BindAddress, picker.getBindAddress(wanEndpointQualifier));

        assertEquals(memberPublicAddress, picker.getPublicAddress(EndpointQualifier.MEMBER));
        assertEquals(clientPublicAddress, picker.getPublicAddress(EndpointQualifier.CLIENT));
        assertEquals(textPublicAddress, picker.getPublicAddress(EndpointQualifier.REST));
        assertEquals(wan1PublicAddress, picker.getPublicAddress(wanEndpointQualifier));
    }

    @Test
    public void testPickAddress_fromNetworkConfig() throws Exception {
        Config config = createNetworkingConfig();
        picker = new DelegatingAddressPicker(new AnAddressProvider(), config, logger);

        //This will assign a bindAddress and a publicAddress
        //The bindAddress is incrementally retried until an available port is found
        //The publicAddress is used as it is
        picker.pickAddress();

        NetworkConfig networkConfig = config.getNetworkConfig();
        //All the picker.getBindAddress(X) calls return the same bind address
        assertAddressBetweenPorts(memberBindAddress, picker.getBindAddress(EndpointQualifier.MEMBER), networkConfig);
        assertAddressBetweenPorts(memberBindAddress, picker.getBindAddress(EndpointQualifier.CLIENT), networkConfig);
        assertAddressBetweenPorts(memberBindAddress, picker.getBindAddress(EndpointQualifier.REST), networkConfig);
        assertAddressBetweenPorts(memberBindAddress, picker.getBindAddress(wanEndpointQualifier), networkConfig);

        assertEquals(memberPublicAddress, picker.getPublicAddress(EndpointQualifier.MEMBER));
        assertEquals(memberPublicAddress, picker.getPublicAddress(EndpointQualifier.CLIENT));
        assertEquals(memberPublicAddress, picker.getPublicAddress(EndpointQualifier.REST));
        assertEquals(memberPublicAddress, picker.getPublicAddress(wanEndpointQualifier));
    }

    public static class AnAddressProvider implements MemberAddressProvider {
        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("127.0.0.1", 2000);
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            switch (qualifier.getType()) {
                case MEMBER:
                    return new InetSocketAddress("127.0.0.1", 2000);
                case CLIENT:
                    return new InetSocketAddress("127.0.0.1", 2001);
                case REST:
                    return new InetSocketAddress("127.0.0.1", 2002);
                case WAN:
                    return new InetSocketAddress("127.0.0.1", 2003);
                default:
                    throw new IllegalStateException();
            }
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("10.10.10.10", 3000);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            switch (qualifier.getType()) {
                case MEMBER:
                    return new InetSocketAddress("10.10.10.10", 3000);
                case CLIENT:
                    return new InetSocketAddress("10.10.10.10", 3001);
                case REST:
                    return new InetSocketAddress("10.10.10.10", 3002);
                case WAN:
                    return new InetSocketAddress("10.10.10.10", 3003);
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private Config createAdvancedNetworkConfig() {
        Config config = new Config();
        config.getAdvancedNetworkConfig().setEnabled(true);
        config.getAdvancedNetworkConfig()
              .getMemberAddressProviderConfig().setEnabled(true).setImplementation(new AnAddressProvider());

        config.getAdvancedNetworkConfig().setMemberEndpointConfig(
                new ServerSocketEndpointConfig().setPort(3000)
        );

        config.getAdvancedNetworkConfig().setClientEndpointConfig(
                new ServerSocketEndpointConfig().setPort(3001)
        );

        config.getAdvancedNetworkConfig().setRestEndpointConfig(
                new RestServerEndpointConfig().setPort(3002)
        );

        config.getAdvancedNetworkConfig().addWanEndpointConfig(
                new ServerSocketEndpointConfig().setName("wan1").setPort(3003)
        );
        return config;
    }

    private Config createNetworkingConfig() {
        Config config = new Config();
        config.getNetworkConfig()
              .getMemberAddressProviderConfig().setEnabled(true).setImplementation(new AnAddressProvider());
        return config;
    }

    static void assertAddressBetweenPorts(Address expected, Address actual, NetworkConfig networkConfig) {
        assertAddressBetweenPorts(expected, actual, networkConfig.isPortAutoIncrement(), networkConfig.getPortCount());
    }

    static void assertAddressBetweenPorts(Address expected, Address actual, boolean isPortAutoIncrement, int portCount) {
        int beginPort = expected.getPort();
        int endPort = beginPort;

        if (isPortAutoIncrement) {
            endPort += portCount;
        }
        assertEquals(expected.getHost(), actual.getHost());
        assertThat(actual.getPort())
                .as("Expected Address %s , Actual Address %s", expected, actual)
                .isBetween(beginPort, endPort);
    }
}
