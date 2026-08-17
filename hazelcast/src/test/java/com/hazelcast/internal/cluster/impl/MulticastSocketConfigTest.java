/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;

import static java.net.StandardSocketOptions.IP_MULTICAST_LOOP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MulticastSocketConfigTest {

    @Test
    public void testLoopbackModeNotNull()
            throws Exception {
        try (MulticastSocket socket = new MulticastSocket(null)) {
            assertThat(socket.getOption(IP_MULTICAST_LOOP)).isNotNull().isTrue();
            assertThatThrownBy(() -> socket.setOption(IP_MULTICAST_LOOP, null)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private MulticastSocket setupSocketMock()
            throws IOException {
        MulticastSocket multicastSocket = mock(MulticastSocket.class);
        when(multicastSocket.getOption(IP_MULTICAST_LOOP)).thenReturn(true);
        return multicastSocket;
    }

    private NetworkInterface getLoopbackInterface()
            throws SocketException {
        for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (ni.isLoopback()) {
                return ni;
            }
        }
        throw new AssertionError("No loopback interface found!");
    }

    private NetworkInterface getNonLoopbackInterface()
            throws SocketException {
        for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!ni.isLoopback()) {
                return ni;
            }
        }
        throw new AssertionError("No non loopback interface found!");
    }

    @Test
    public void testSetInterfaceForced()
            throws Exception {
        Config config = createConfig(Boolean.TRUE);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = setupSocketMock();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        NetworkInterface loopback = getLoopbackInterface();
        MulticastService.configureMulticastSocket(multicastSocket, loopback, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setNetworkInterface(loopback);
    }

    @Test
    public void testSetInterfaceDisabled()
            throws Exception {
        Config config = createConfig(Boolean.FALSE);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = setupSocketMock();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        NetworkInterface loopback = getLoopbackInterface();
        MulticastService.configureMulticastSocket(multicastSocket, loopback, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket, never()).setInterface(any());
        verify(multicastSocket, never()).setNetworkInterface(any());
        InetAddress groupAddress = InetAddress.getByName(multicastConfig.getMulticastGroup());
        verify(multicastSocket).joinGroup(new InetSocketAddress(groupAddress, 0), null);
    }

    @Test
    public void testSetInterfaceDefaultWhenLoopback()
            throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        multicastConfig.setLoopbackModeEnabled(true);
        MulticastSocket multicastSocket = setupSocketMock();
        NetworkInterface loopback = getLoopbackInterface();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, loopback, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setOption(IP_MULTICAST_LOOP, true);
        verify(multicastSocket).setNetworkInterface(loopback);
    }

    @Test
    public void testSetInterfaceDefaultWhenNonLoopbackAddrAndLoopbackMode()
            throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        multicastConfig.setLoopbackModeEnabled(true);
        MulticastSocket multicastSocket = setupSocketMock();
        NetworkInterface ni = getNonLoopbackInterface();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, ni, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setNetworkInterface(ni);
        verify(multicastSocket).setOption(IP_MULTICAST_LOOP, true);
    }

    @Test
    public void testSetInterfaceDefaultWhenNonLoopbackAddrAndNoLoopbackMode()
            throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        multicastConfig.setLoopbackModeEnabled(false);
        MulticastSocket multicastSocket = setupSocketMock();
        NetworkInterface ni = getNonLoopbackInterface();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, ni, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setNetworkInterface(ni);
        verify(multicastSocket).setOption(IP_MULTICAST_LOOP, false);

    }

    /**
     * Verifes the {@link MulticastSocket#setInterface(InetAddress)} is called by default if non-loopback address is used.
     * This is a regression test for the <a href="https://github.com/hazelcast/hazelcast/issues/19192">issue #19192</a>
     * (hit on Mac OS).
     */
    @Test
    public void testSetInterfaceDefaultWhenNonLoopbackAddrAndDefaultLoopbackMode()
            throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = setupSocketMock();
        NetworkInterface ni = getNonLoopbackInterface();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, ni, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket).setNetworkInterface(ni);
    }

    @Test
    public void testMulticastParams()
            throws Exception {
        Config config = createConfig(null);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = setupSocketMock();
        NetworkInterface ni = getNonLoopbackInterface();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, ni, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket).bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
        verify(multicastSocket).setTimeToLive(multicastConfig.getMulticastTimeToLive());
        verify(multicastSocket, never()).setLoopbackMode(anyBoolean());
        InetAddress groupAddress = InetAddress.getByName(multicastConfig.getMulticastGroup());
        verify(multicastSocket).joinGroup(new InetSocketAddress(groupAddress, 0), null);
    }

    @Test
    public void testMulticastGroupProperty()
            throws Exception {
        Config config = createConfig(null);
        String customMulticastGroup = "225.225.225.225";
        config.setProperty(ClusterProperty.MULTICAST_GROUP.getName(), customMulticastGroup);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        MulticastSocket multicastSocket = setupSocketMock();
        NetworkInterface ni = getNonLoopbackInterface();
        HazelcastProperties hzProperties = new HazelcastProperties(config);
        MulticastService.configureMulticastSocket(multicastSocket, ni, hzProperties, multicastConfig, mock(ILogger.class));
        verify(multicastSocket).bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
        verify(multicastSocket).setTimeToLive(multicastConfig.getMulticastTimeToLive());
        verify(multicastSocket, never()).setOption(eq(IP_MULTICAST_LOOP), anyBoolean());
        verify(multicastSocket).joinGroup(new InetSocketAddress(InetAddress.getByName(customMulticastGroup), 0), null);
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
