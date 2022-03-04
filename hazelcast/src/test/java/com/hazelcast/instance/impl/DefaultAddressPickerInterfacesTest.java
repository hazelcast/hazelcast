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

package com.hazelcast.instance.impl;

import com.hazelcast.config.Config;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.internal.util.DummyNetworkInterfacesEnumerator;
import com.hazelcast.internal.util.NetworkInterfaceInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;

import static com.hazelcast.instance.impl.DefaultAddressPicker.PREFER_IPV4_STACK;
import static com.hazelcast.instance.impl.DefaultAddressPicker.PREFER_IPV6_ADDRESSES;
import static com.hazelcast.instance.impl.TestUtil.setSystemProperty;
import static com.hazelcast.internal.util.NetworkInterfaceInfo.builder;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Tests if the {@link DefaultAddressPicker} chooses an expected bind address.
 * <br/>
 * Given: The default Hazelcast Config is used and no Interface definition network configuration is set.
 * The System property {@link DefaultAddressPicker#PREFER_IPV4_STACK} is set to {@code true}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultAddressPickerInterfacesTest {

    private final ILogger logger = Logger.getLogger(AddressPicker.class);
    private final Config config = new Config();

    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv4 = set(PREFER_IPV4_STACK, "true");
    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv6 = clear(PREFER_IPV6_ADDRESSES);
    @Rule
    public final OverridePropertyRule ruleSysPropHzPreferIpv4 = set(ClusterProperty.PREFER_IPv4_STACK.getName(), "false");

    private DummyNetworkInterfacesEnumerator networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator();

    /**
     * When: First network interface is a loopback and the other is a non-loopback.<br>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testLoopbackFirst() throws Exception {
        NetworkInterfaceInfo lo = builder("lo").withLoopback(true).withAddresses("127.0.0.1").build();
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("192.168.1.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(lo, eth0);
        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is a loopback and the other is a non-loopback.<br>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testLoopbackLast() throws Exception {
        NetworkInterfaceInfo lo = builder("lo").withLoopback(true).withAddresses("127.0.0.1").build();
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("192.168.1.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0, lo);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: First network interface is DOWN and the other is UP.<br>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceDownFirst() throws Exception {
        NetworkInterfaceInfo docker = builder("docker").withUp(false).withAddresses("172.17.0.1").build();
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("192.168.1.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(docker, eth0);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is DOWN and the other is UP.<br>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceDownLast() throws Exception {
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("192.168.1.1").build();
        NetworkInterfaceInfo docker = builder("docker").withUp(false).withAddresses("172.17.0.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0, docker);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: First network interface is virtual and the other is not virtual.<br>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceVirtualFirst() throws Exception {
        NetworkInterfaceInfo virtualEth0 = builder("eth0:0").withVirtual(true).withAddresses("172.17.0.1").build();
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("192.168.1.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(virtualEth0, eth0);


        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is virtual and the other is not virtual.<br>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceVirtualLast() throws Exception {
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("192.168.1.1").build();
        NetworkInterfaceInfo virtualEth0 = builder("eth0:0").withVirtual(true).withAddresses("172.17.0.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0, virtualEth0);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: No network interface is provided.<br>
     * Then: The address picker returns {@code null} as the picked address.
     */
    @Test
    public void testNoInterface() throws Exception {
        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNull("Null InetAddress is expected when NetworkInterface enumeration is empty", inetAddress);
    }

    /**
     * When: There is no available interface/address combination for picking.<br>
     * Then: The address picker returns {@code null} as the picked address.
     */
    @Test
    public void testNoAddress() throws Exception {
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses().build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNull("Null InetAddress is expected when the available NetworkInterface has no address", inetAddress);

        NetworkInterfaceInfo docker = builder("docker").withUp(false).withAddresses("172.17.0.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0, docker);

        inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNull("Null InetAddress is expected when the available NetworkInterface has no address", inetAddress);
    }

    /**
     * When: Fist network interface is valid for picking, but has no InetAddress.<br>
     * Then: Another interface will be used for address picking.
     */
    @Test
    public void testNoAddressFirst() throws Exception {
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses().build();
        NetworkInterfaceInfo eth1 = builder("eth1").withAddresses("192.168.1.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0, eth1);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is valid for picking, but has no InetAddress.<br>
     * Then: Another interface will be used for address picking.
     */
    @Test
    public void testNoAddressLast() throws Exception {
        NetworkInterfaceInfo eth1 = builder("eth1").withAddresses("192.168.1.1").build();
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses().build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth1, eth0);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: A valid Network interface has more addresses.<br>
     * Then: One of the addresses is picked.
     */
    @Test
    public void testMoreAddresses() throws Exception {
        NetworkInterfaceInfo lo = builder("lo").withLoopback(true).withAddresses("127.0.0.1").build();
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("192.168.1.1", "172.172.172.172").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(lo, eth0);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertThat(inetAddress.getHostAddress(), anyOf(equalTo("192.168.1.1"), equalTo("172.172.172.172")));
    }

    /**
     * When: Network interface has both IPv4 and IPv6 addresses and IPv4 is preferred.<br>
     * Then: The IPv4 address is picked.
     */
    @Test
    public void testIPv4Preferred() throws Exception {
        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("fe80::9711:82f4:383a:e254", "192.168.1.1", "::cace").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Network interface has both IPv4 and IPv6 addresses and IPv6 is preferred.<br>
     * Then: The IPv6 address is picked.
     */
    @Test
    public void testIPv6Preferred() throws Exception {
        setSystemProperty(PREFER_IPV4_STACK, "false");
        setSystemProperty(PREFER_IPV6_ADDRESSES, "true");

        NetworkInterfaceInfo eth0 = builder("eth0").withAddresses("fe80::9711:82f4:383a:e254", "172.17.0.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(eth0);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("fe80:0:0:0:9711:82f4:383a:e254", inetAddress.getHostAddress());
    }

    /**
     * When: Multiple interfaces with different configuration is used, but only one IPv4 interface/address combination is
     * pickable.<br>
     * Then: The correct address is picked.
     */
    @Test
    public void testComplexScenario() throws Exception {
        NetworkInterfaceInfo lo = builder("lo").withLoopback(true).withAddresses("127.0.0.1", "::1").build();
        NetworkInterfaceInfo docker0 = builder("docker0").withUp(false).withAddresses("172.17.0.1").build();
        NetworkInterfaceInfo wlp3s0 = builder("wlp3s0").withUp(false).build();
        NetworkInterfaceInfo eth00 = builder("eth0:0").withVirtual(true).withAddresses("8.8.8.8", "8.8.4.4").build();
        NetworkInterfaceInfo enp0s25 = builder("enp0s25").withAddresses("fe80::9711:82f4:383a:e254", "192.168.1.4").build();
        NetworkInterfaceInfo virbr1 = builder("virbr1").withUp(false).withAddresses("192.168.42.1").build();
        networkInterfacesEnumerator = new DummyNetworkInterfacesEnumerator(lo, docker0, wlp3s0, eth00, enp0s25, virbr1);

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.4", inetAddress.getHostAddress());
    }

    /**
     * This method executes {@code DefaultAddressPicker.pickMatchingAddress()}
     * and returns the {@code inetAddress} field from the method result
     * or {@code null} if the result was {@code null}.
     */
    private InetAddress getInetAddressFromDefaultAddressPicker() throws Exception {
        DefaultAddressPicker picker = new DefaultAddressPicker(config, logger);
        picker.setNetworkInterfacesEnumerator(networkInterfacesEnumerator);
        DefaultAddressPicker.AddressDefinition addressDefinition = picker.pickMatchingAddress(null);
        return addressDefinition == null ? null : addressDefinition.inetAddress;
    }

}
