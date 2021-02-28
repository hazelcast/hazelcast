/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TranslateToPublicAddressProviderTest {
    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
    public static final String REACHABLE_HOST = "127.0.0.1";
    public static final String UNREACHABLE_HOST = "192.168.0.1";

    private final ClientConfig config = new ClientConfig();

    @After
    public void teardown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void nonDefaultAddressProvider() {
        // given
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(new TestAddressProvider(), null);
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void propertyTrue() {
        // given
        config.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.toString(), "true");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), null);
        boolean result = translateProvider.get();

        // then
        assertTrue(result);
    }

    @Test
    public void propertyFalse() {
        // given
        config.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.toString(), "false");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), null);
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig() {
        // given
        config.getNetworkConfig().addAddress("127.0.0.1");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), asList(member("192.168.0.1"), member("127.0.0.1")));
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig_memberUsesHostName() {
        // given
        config.getNetworkConfig().addAddress("127.0.0.1");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), asList(member("localhost")));
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig_clientUsesHostname() {
        // given
        config.getNetworkConfig().addAddress("localhost");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), asList(member("127.0.0.1")));
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig_clientUsesHostnameAndIp() {
        // given
        config.getNetworkConfig().addAddress("localhost:5701");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), asList(member("127.0.0.1")));
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void membersWithoutPublicAddresses() {
        // given
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), asList(member("127.0.0.1")));
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void membersReachableViaInternalAddress() {
        // given
        Hazelcast.newHazelcastInstance();
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(),
                asList(member(REACHABLE_HOST, UNREACHABLE_HOST), member(REACHABLE_HOST, UNREACHABLE_HOST)));
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void membersUnreachable() {
        // given
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), asList(member(UNREACHABLE_HOST, UNREACHABLE_HOST)));
        boolean result = translateProvider.get();

        // then
        assertFalse(result);
    }

    @Test
    public void membersReachableOnlyViaPublicAddress() {
        // given
        Hazelcast.newHazelcastInstance();
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.refresh(defaultAddressProvider(), asList(member(UNREACHABLE_HOST, REACHABLE_HOST)));
        boolean result = translateProvider.get();

        // then
        assertTrue(result);
    }

    @Nonnull
    private TranslateToPublicAddressProvider createTranslateProvider() {
        return new TranslateToPublicAddressProvider(config.getNetworkConfig(),
                new HazelcastProperties(config.getProperties()), mock(ILogger.class));
    }

    @Nonnull
    private DefaultAddressProvider defaultAddressProvider() {
        return new DefaultAddressProvider(config.getNetworkConfig());
    }

    @Nonnull
    private MemberInfo member(String host) {
        try {
            return new MemberInfo(new Address(host, 5701), UUID.randomUUID(), emptyMap(), false, VERSION);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private MemberInfo member(String host, String publicHost) {
        try {
            Address internalAddress = new Address(host, 5701);
            Address publicAddress = new Address(publicHost, 5701);
            return new MemberInfo(internalAddress, UUID.randomUUID(), emptyMap(), false, VERSION,
                    singletonMap(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"), publicAddress));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private class TestAddressProvider implements AddressProvider {
        @Override
        public Address translate(Address address) {
            return address;
        }

        @Override
        public Addresses loadAddresses() {
            return new Addresses(emptyList());
        }
    }
}
