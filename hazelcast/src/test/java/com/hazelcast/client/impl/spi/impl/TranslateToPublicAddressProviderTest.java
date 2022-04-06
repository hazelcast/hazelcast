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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
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
    public void propertyTrue() {
        // given
        config.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.toString(), "true");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class), emptySet()));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertTrue(result);
    }

    @Test
    public void propertyFalse() {
        // given
        config.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.toString(), "false");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class), emptySet()));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig() {
        // given
        config.getNetworkConfig().addAddress("127.0.0.1");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(asList(member("192.168.0.1"), member("127.0.0.1")))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig_memberUsesHostName() {
        // given
        config.getNetworkConfig().addAddress("127.0.0.1");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(Collections.singletonList(member("localhost")))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig_clientUsesHostname() {
        // given
        config.getNetworkConfig().addAddress("localhost");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(Collections.singletonList(member("127.0.0.1")))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void memberInternalAddressAsDefinedInClientConfig_clientUsesHostnameAndIp() {
        // given
        config.getNetworkConfig().addAddress("localhost:5701");
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(Collections.singletonList(member("127.0.0.1")))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void membersWithoutPublicAddresses() {
        // given
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(Collections.singletonList(member("127.0.0.1")))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void membersReachableViaInternalAddress() {
        // given
        Hazelcast.newHazelcastInstance();
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(asList(member(REACHABLE_HOST, UNREACHABLE_HOST),
                        member(REACHABLE_HOST, UNREACHABLE_HOST)))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void membersUnreachable() {
        // given
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(Collections.singletonList(member(UNREACHABLE_HOST, UNREACHABLE_HOST)))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertFalse(result);
    }

    @Test
    public void membersReachableOnlyViaPublicAddress() {
        // given
        Hazelcast.newHazelcastInstance();
        TranslateToPublicAddressProvider translateProvider = createTranslateProvider();

        // when
        translateProvider.init(new InitialMembershipEvent(mock(Cluster.class),
                new HashSet<>(Collections.singletonList(member(UNREACHABLE_HOST, REACHABLE_HOST)))));
        boolean result = translateProvider.getAsBoolean();

        // then
        assertTrue(result);
    }

    @Nonnull
    private TranslateToPublicAddressProvider createTranslateProvider() {
        return new TranslateToPublicAddressProvider(config.getNetworkConfig(),
                new HazelcastProperties(config.getProperties()), mock(ILogger.class));
    }

    @Nonnull
    private Member member(String host) {
        try {
            MemberImpl.Builder memberBuilder;
            memberBuilder = new MemberImpl.Builder(new Address(host, 5701));
            return memberBuilder.version(VERSION)
                    .uuid(UUID.randomUUID())
                    .attributes(emptyMap())
                    .liteMember(false).build();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private Member member(String host, String publicHost) {

        try {
            MemberImpl.Builder memberBuilder;

            Address internalAddress = new Address(host, 5701);
            Address publicAddress = new Address(publicHost, 5701);

            Map<EndpointQualifier, Address> addressMap = new HashMap<>();
            addressMap.put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"), publicAddress);
            addressMap.put(EndpointQualifier.MEMBER, internalAddress);

            memberBuilder = new MemberImpl.Builder(addressMap);
            return memberBuilder.version(VERSION)
                    .uuid(UUID.randomUUID())
                    .attributes(emptyMap())
                    .liteMember(false).build();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

}
