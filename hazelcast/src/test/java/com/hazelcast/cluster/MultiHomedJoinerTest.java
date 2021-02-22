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
package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.spi.MemberAddressProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * tests multi-homed systems with wildcard addressing
 * Connecting through different IP addresses for the same host should
 * either succeed or fail, but not "hang"
 *
 * @author lprimak
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MultiHomedJoinerTest extends HazelcastTestSupport {
    private TestHazelcastInstanceFactory fact;
    private static final List<String> networks = new ArrayList<>();

    @BeforeClass
    public static void init() throws SocketException {
        System.setProperty(HAZELCAST_TEST_USE_NETWORK, Boolean.TRUE.toString());
        for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!iface.isLoopback() && !iface.isPointToPoint() && iface.isUp() && !iface.isVirtual()
                    && !iface.getName().equals("vboxnet0") && !iface.getName().equals("docker0")
                    && !iface.getDisplayName().contains("Teredo")) {
                iface.getInterfaceAddresses().stream().map(InterfaceAddress::getAddress)
                        .filter(addr -> addr instanceof Inet4Address).findFirst().map(InetAddress::getHostAddress)
                        .ifPresent(networks::add);
            }
        }
    }

    @Before
    public void beforeRun() {
        fact = createHazelcastInstanceFactory(isMultiHomed() ? 3 : 2);
    }

    @After
    public void afterRun() {
        fact.shutdownAll();
    }

    Config getConfig(String hostname) {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setMemberAddressProviderConfig(new MemberAddressProviderConfig().setEnabled(true)
                .setImplementation(new MemberAddressProviderImpl()));
        config.getNetworkConfig().getJoin().setMulticastConfig(new MulticastConfig().setEnabled(false));

        TcpIpConfig tcpIpConfig = new TcpIpConfig();
        tcpIpConfig.setEnabled(true);
        if (hostname != null) {
            tcpIpConfig.addMember(hostname);
        }
        config.getNetworkConfig().getJoin().setTcpIpConfig(tcpIpConfig);
        return config;
    }

    /**
     * I believe that {@link #simulateMultiHomedWithLocalHost()} would be enough
     * when the {@link #multiHomedConnectWithWildcardBind()} is fixed to be passing.
     * At that time, this test can be removed and just the {@link #simulateMultiHomedWithLocalHost()}
     * can be left for the future
     * For now, this test makes sure that multi-homed setup actually gets tested before this test passes.
     */
    @Test
    public void checkMultiHomed() {
        assertGreaterOrEquals("Not Multi-Homed", networks.size(), 2);
    }

    @Test(timeout = 15 * 1000)
    public void multiHomedConnectWithWildcardBind() {
        assumeTrue(isMultiHomed());
        HazelcastInstance hz1 = fact.newHazelcastInstance(getConfig(null));
        HazelcastInstance hz2 = fact.newHazelcastInstance(getConfig(networks.get(0)));
        HazelcastInstance hz3 = fact.newHazelcastInstance(getConfig(networks.get(1)));
        waitUntilClusterState(hz3, ClusterState.ACTIVE, 1);
        assertEquals("cluster not correct size", 3, hz1.getCluster().getMembers().size());
    }

    @Test(timeout = 15 * 1000)
    public void simulateMultiHomedWithLocalHost() {
        HazelcastInstance hz1 = fact.newHazelcastInstance(getConfig(null));
        HazelcastInstance hz2 = fact.newHazelcastInstance(getConfig(networks.get(0)));
        waitUntilClusterState(hz2, ClusterState.ACTIVE, 1);
        assertEquals("cluster not correct size", 2, hz1.getCluster().getMembers().size());
    }


    private static boolean isMultiHomed() {
        return networks.size() >= 2;
    }

    private static class MemberAddressProviderImpl implements MemberAddressProvider {
        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress(0);
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return getBindAddress();
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress(isMultiHomed() ? networks.get(0) : "127.0.0.1", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return getPublicAddress();
        }
    }
}
