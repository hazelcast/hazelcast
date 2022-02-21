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

package com.hazelcast.internal.cluster.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class MulticastServiceTest {

    @Test
    public void testSetInterfaceForced() throws Exception {
        Config config = createConfig(Boolean.TRUE);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("127.0.0.1",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setInterface(address.getInetAddress());
    }

    @Test
    public void testSetInterfaceDisabled() throws Exception {
        Config config = createConfig(Boolean.FALSE);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("127.0.0.1",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket, never()).setInterface(any());
    }

    @Test
    public void testSetInterfaceDefaultWhenLoopback() throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        multicastConfig.setLoopbackModeEnabled(true);
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("127.0.0.1",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setLoopbackMode(false);
        // https://github.com/hazelcast/hazelcast/pull/19251#issuecomment-891375270
        if (OsHelper.isMac()) {
            verify(multicastSocket).setInterface(address.getInetAddress());
        } else {
            verify(multicastSocket, never()).setInterface(any());
        }
    }

    @Test
    public void testSetInterfaceDefaultWhenNonLoopbackAddrAndLoopbackMode() throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        multicastConfig.setLoopbackModeEnabled(true);
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("10.0.0.2",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setInterface(address.getInetAddress());
        verify(multicastSocket).setLoopbackMode(false);
    }

    @Test
    public void testSetInterfaceDefaultWhenNonLoopbackAddrAndNoLoopbackMode() throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        multicastConfig.setLoopbackModeEnabled(false);
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("10.0.0.2",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setInterface(address.getInetAddress());
        verify(multicastSocket).setLoopbackMode(true);

    }

    /**
     * Verifes the {@link MulticastSocket#setInterface(InetAddress)} is called by default if non-loopback address is used.
     * This is a regression test for the <a href="https://github.com/hazelcast/hazelcast/issues/19192">issue #19192</a>
     * (hit on Mac OS).
     */
    @Test
    public void testSetInterfaceDefaultWhenNonLoopbackAddrAndDefaultLoopbackMode() throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("10.0.0.2",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setInterface(address.getInetAddress());
    }

    @Test
    public void testMulticastParams() throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("10.0.0.2",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket).bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
        verify(multicastSocket).setTimeToLive(multicastConfig.getMulticastTimeToLive());
        verify(multicastSocket, never()).setLoopbackMode(anyBoolean());
        verify(multicastSocket).joinGroup(InetAddress.getByName(multicastConfig.getMulticastGroup()));
    }

    @Test
    public void testMulticastGroupProperty() throws Exception {
        Config config = createConfig(null);
        String customMulticastGroup = "225.225.225.225";
        config.setProperty(ClusterProperty.MULTICAST_GROUP.getName(), customMulticastGroup);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        Address address = new Address("10.0.0.2",  5701);
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, address, hzProperties , multicastConfig, mock(ILogger.class));
        verify(multicastSocket).bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
        verify(multicastSocket).setTimeToLive(multicastConfig.getMulticastTimeToLive());
        verify(multicastSocket, never()).setLoopbackMode(anyBoolean());
        verify(multicastSocket).joinGroup(InetAddress.getByName(customMulticastGroup));
    }

    private Config createConfig(Boolean callSetInterface) {
        Config config = new Config();
        if (callSetInterface != null) {
            config.setProperty(ClusterProperty.MULTICAST_SOCKET_SET_INTERFACE.getName(), callSetInterface.toString());
        }
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getAutoDetectionConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(false);
        MulticastConfig multicastConfig = joinConfig.getMulticastConfig();
        multicastConfig.setEnabled(true).setMulticastGroup("239.1.2.3").setMulticastPort(8686).setMulticastTimeoutSeconds(3)
                .setMulticastTimeToLive(0).addTrustedInterface("192.168.1.1");
        return config;
    }
}
