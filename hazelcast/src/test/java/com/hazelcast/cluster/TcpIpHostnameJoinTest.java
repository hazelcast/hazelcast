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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.DefaultAddressPicker;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static java.util.Collections.enumeration;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DefaultAddressPicker.class)
@PowerMockIgnore("javax.management.*")
@Category(SlowTest.class)
public class TcpIpHostnameJoinTest {

    private static final InetAddress LOCALHOST;
    private static final InetAddress LOCALHOST2;
    private static final InetAddress LOCALHOST3;
    private static final InetAddress LOCALHOST4;

    static {
        try {
            LOCALHOST = InetAddress.getByAddress("localhost", new byte[] {127, 0, 0, 1});
            LOCALHOST2 = InetAddress.getByAddress("localhost2", new byte[] {127, 0, 0, 1});
            LOCALHOST3 = InetAddress.getByAddress("localhost3", new byte[] {127, 0, 0, 1});
            LOCALHOST4 = InetAddress.getByAddress("localhost4", new byte[] {127, 0, 0, 1});
        } catch (UnknownHostException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        mockStatic(NetworkInterface.class);

        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig("lo", true, LOCALHOST, LOCALHOST2, LOCALHOST3, LOCALHOST4));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(enumeration(networkInterfaces));
    }

    @After
    public void after() throws Exception {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void test_whenMembersSpecifiedViaHostnames() throws Exception {
        HazelcastInstance hz1 = instance("localhost");
        HazelcastInstance hz2 = instance("localhost2");

        assertClusterSize(2, hz1, hz2);
    }

    @Test
    public void test_whenMembersDefinedViaIpAndHostnameMix() {
        HazelcastInstance hz1 = instance("127.0.0.1");
        HazelcastInstance hz2 = instance("localhost");

        assertClusterSize(2, hz1, hz2);
    }

    @Test
    public void test_whenMembersDefinedHostnamesFormIndependentClusters() {
        HazelcastInstance hz1 = instance("cluster1", "localhost3");
        HazelcastInstance hz2 = instance("cluster2", "localhost4");
        //mocking doesn't always work properly if I use "localhost" and "localhost2" here...

        assertClusterSize(1, hz1);
        assertClusterSize(1, hz2);
    }

    private HazelcastInstance instance(String hostnameOrIp) {
        return instance("cluster", hostnameOrIp);
    }

    private HazelcastInstance instance(String cluster, String hostnameOrIp) {
        Config config = new Config();

        config.setClusterName(cluster);

        config.getMetricsConfig().setEnabled(false);
        config.getMetricsConfig().getJmxConfig().setEnabled(false);
        config.setProperty("hazelcast.logging.type", "log4j2");

        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).clear().addMember(hostnameOrIp);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static NetworkInterface createNetworkConfig(String name, boolean loopback, InetAddress... addresses) throws IOException {
        NetworkInterface networkInterface = mock(NetworkInterface.class);
        when(networkInterface.getName()).thenReturn(name);
        when(networkInterface.isUp()).thenReturn(true);
        when(networkInterface.isLoopback()).thenReturn(loopback);
        when(networkInterface.isVirtual()).thenReturn(false);
        when(networkInterface.getInetAddresses()).thenReturn(enumeration(Arrays.asList(addresses)));
        return networkInterface;
    }

}
