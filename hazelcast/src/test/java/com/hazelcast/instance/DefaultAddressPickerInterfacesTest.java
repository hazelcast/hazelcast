/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import static com.hazelcast.instance.DefaultAddressPickerInterfacesTest.NetworkInterfaceOptions.builder;
import static com.hazelcast.instance.TestUtil.setSystemProperty;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * This class contains PowerMock driven tests which emulate different NetworkInterfaces configurations. The tests check if the
 * DefaultAdddressPicker chooses an expected bind address.
 * <p>
 * Given: Default Hazelcast Config is used and no Interface definition network configuration is set. No system property which
 * could influence address picking is used.
 * </p>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DefaultAddressPicker.class })
@Category({ QuickTest.class, ParallelTest.class })
public class DefaultAddressPickerInterfacesTest {

    private static final String SYS_PROP_PREFER_IPV4 = "java.net.preferIPv4Stack";
    private final ILogger logger = Logger.getLogger(AddressPicker.class);
    private final Config config = new Config();
    private final HazelcastProperties hazelcastProperties = new HazelcastProperties(config);

    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv4 = set(SYS_PROP_PREFER_IPV4, "true");
    @Rule
    public final OverridePropertyRule ruleSysPropHzPreferIpv4 = set("hazelcast.prefer.ipv4.stack", "false");

    /**
     * Enable NetworkInterface static methods mocking before each tests.
     */
    @Before
    public void before() {
        PowerMockito.mockStatic(NetworkInterface.class);
    }

    /**
     * When: First network interface is a loopback and the other is a non-loopback.<br/>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testLoopbackFirst() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces
                .add(createNetworkConfig(builder().withName("lo").withLoopback(true).withAddresses("127.0.0.1").build()));
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses("192.168.1.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is a loopback and the other is a non-loopback.<br/>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testLoopbackLast() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses("192.168.1.1").build()));
        networkInterfaces
                .add(createNetworkConfig(builder().withName("lo").withLoopback(true).withAddresses("127.0.0.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: First network interface is DOWN and the other is UP.<br/>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceDownFirst() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces
                .add(createNetworkConfig(builder().withName("docker").withUp(false).withAddresses("172.17.0.1").build()));
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses("192.168.1.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is DOWN and the other is UP.<br/>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceDownLast() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses("192.168.1.1").build()));
        networkInterfaces
                .add(createNetworkConfig(builder().withName("docker").withUp(false).withAddresses("172.17.0.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: First network interface is virtual and the other is not virtual.<br/>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceVirtualFirst() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces
                .add(createNetworkConfig(builder().withName("eth0:0").withVirtual(true).withAddresses("172.17.0.1").build()));
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses("192.168.1.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is virtual and the other is not virtual.<br/>
     * Then: The other interface will be used for address picking.
     */
    @Test
    public void testInterfaceVirtualLast() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses("192.168.1.1").build()));
        networkInterfaces
                .add(createNetworkConfig(builder().withName("eth0:0").withVirtual(true).withAddresses("172.17.0.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: No network interface is provided.<br/>
     * Then: The address picker returns {@code null} as the picked address.
     */
    @Test
    public void testNoInterface() throws Exception {
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(Collections.EMPTY_LIST));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNull("Null InetAddress is expected when NetworkInterface enumeration is empty", inetAddress);
    }

    /**
     * When: There is no available interface/address combination for picking.<br/>
     * Then: The address picker returns {@code null} as the picked address.
     */
    @Test
    public void testNoAddress() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses().build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNull("Null InetAddress is expected when the available NetworkInterface has no address", inetAddress);

        networkInterfaces
                .add(createNetworkConfig(builder().withName("docker").withUp(false).withAddresses("172.17.0.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNull("Null InetAddress is expected when the available NetworkInterface has no address", inetAddress);
    }

    /**
     * When: Fist network interface is valid for picking, but has no InetAddress.<br/>
     * Then: Another interface will be used for address picking.
     */
    @Test
    public void testNoAddressFirst() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses().build()));
        networkInterfaces.add(createNetworkConfig(builder().withName("eth1").withAddresses("192.168.1.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Last network interface is valid for picking, but has no InetAddress.<br/>
     * Then: Another interface will be used for address picking.
     */
    @Test
    public void testNoAddressLast() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig(builder().withName("eth1").withAddresses("192.168.1.1").build()));
        networkInterfaces.add(createNetworkConfig(builder().withName("eth0").withAddresses().build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: A valid Network interface has more addresses.<br/>
     * Then: One of the addresses is picked.
     */
    @Test
    public void testMoreAddresses() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces
                .add(createNetworkConfig(builder().withName("lo").withLoopback(true).withAddresses("127.0.0.1").build()));
        networkInterfaces
                .add(createNetworkConfig(builder().withName("eth0").withAddresses("192.168.1.1", "172.172.172.172").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertThat(inetAddress.getHostAddress(), anyOf(equalTo("192.168.1.1"), equalTo("172.172.172.172")));
    }

    /**
     * When: Network interface has both IPv4 and IPv6 addresses and IPv4 is preferred.<br/>
     * Then: The IPv4 address is picked.
     */
    @Test
    public void testIPv4Preferred() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(createNetworkConfig(
                builder().withName("eth0").withAddresses("fe80::9711:82f4:383a:e254", "192.168.1.1", "::cace").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.1", inetAddress.getHostAddress());
    }

    /**
     * When: Network interface has both IPv4 and IPv6 addresses and IPv6 is preferred.<br/>
     * Then: The IPv6 address is picked.
     */
    @Test
    public void testIPv6Preferred() throws Exception {
        String origPref = setSystemProperty(SYS_PROP_PREFER_IPV4, "false");
        try {
            List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
            networkInterfaces.add(createNetworkConfig(
                    builder().withName("eth0").withAddresses("fe80::9711:82f4:383a:e254", "172.17.0.1").build()));
            when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

            InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
            assertNotNull("Not-null InetAddress is expected", inetAddress);
            assertEquals("fe80:0:0:0:9711:82f4:383a:e254", inetAddress.getHostAddress());
        } finally {
            setSystemProperty(SYS_PROP_PREFER_IPV4, origPref);
        }
    }

    /**
     * When: Multiple interfaces with different configuration is used, but only one IPv4 interface/address combination is
     * pickable.<br/>
     * Then: The correct address is picked.
     */
    @Test
    public void testComplexScenario() throws Exception {
        List<NetworkInterface> networkInterfaces = new ArrayList<NetworkInterface>();
        networkInterfaces.add(
                createNetworkConfig(builder().withName("lo").withLoopback(true).withAddresses("127.0.0.1", "::1").build()));
        networkInterfaces
                .add(createNetworkConfig(builder().withName("docker0").withUp(false).withAddresses("172.17.0.1").build()));
        networkInterfaces.add(createNetworkConfig(builder().withName("wlp3s0").withUp(false).build()));
        networkInterfaces.add(createNetworkConfig(
                builder().withName("eth0:0").withVirtual(true).withAddresses("8.8.8.8", "8.8.4.4").build()));
        networkInterfaces.add(createNetworkConfig(
                builder().withName("enp0s25").withAddresses("fe80::9711:82f4:383a:e254", "192.168.1.4").build()));
        networkInterfaces
                .add(createNetworkConfig(builder().withName("virbr1").withUp(false).withAddresses("192.168.42.1").build()));
        when(NetworkInterface.getNetworkInterfaces()).thenReturn(Collections.enumeration(networkInterfaces));

        InetAddress inetAddress = getInetAddressFromDefaultAddressPicker();
        assertNotNull("Not-null InetAddress is expected", inetAddress);
        assertEquals("192.168.1.4", inetAddress.getHostAddress());
    }

    /**
     * This method uses reflection to execute {@code DefaultAddressPicker.pickMatchingAddress()} and to return
     * {@code inetAddress} field from the method result object.
     */
    private InetAddress getInetAddressFromDefaultAddressPicker() throws Exception {
        DefaultAddressPicker picker = new DefaultAddressPicker(config, hazelcastProperties, logger);
        Method method = DefaultAddressPicker.class.getDeclaredMethod("pickMatchingAddress", Collection.class);
        method.setAccessible(true);
        Object addressDefinitionObject = method.invoke(picker, new Object[] { null });
        if (addressDefinitionObject == null) {
            return null;
        }
        Field field = addressDefinitionObject.getClass().getDeclaredField("inetAddress");
        field.setAccessible(true);
        InetAddress inetAddress = (InetAddress) field.get(addressDefinitionObject);
        return inetAddress;
    }

    /**
     * Creates a mocked NetworkInterface instance with given configuration.
     */
    private NetworkInterface createNetworkConfig(NetworkInterfaceOptions networkConfigOptions) throws IOException {
        NetworkInterface networkInterface = PowerMockito.mock(NetworkInterface.class);
        when(networkInterface.getName()).thenReturn(networkConfigOptions.name);
        when(networkInterface.isUp()).thenReturn(networkConfigOptions.up);
        when(networkInterface.isLoopback()).thenReturn(networkConfigOptions.loopback);
        when(networkInterface.isVirtual()).thenReturn(networkConfigOptions.virtual);
        when(networkInterface.getInetAddresses()).thenReturn(createInetAddrs(networkConfigOptions.addresses));
        return networkInterface;
    }

    /**
     * From given String array creates an enumeration of {@link InetAddress} instances.
     */
    private Enumeration<InetAddress> createInetAddrs(String[] addresses) throws UnknownHostException {
        List<InetAddress> inetAddresses = new ArrayList<InetAddress>();
        for (String addr : addresses) {
            inetAddresses.add(InetAddress.getByName(addr));
        }
        return Collections.enumeration(inetAddresses);
    }

    /**
     * Configuration object for {@link NetworkInterface} mocking.
     */
    public static class NetworkInterfaceOptions {
        private final String name;
        private final boolean up;
        private final boolean loopback;
        private final boolean virtual;
        private final String[] addresses;

        private NetworkInterfaceOptions(Builder builder) {
            this.name = checkNotNull(builder.name);
            this.up = builder.up;
            this.loopback = builder.loopback;
            this.virtual = builder.virtual;
            this.addresses = checkNotNull(builder.addresses);
        }

        /**
         * Creates builder to build {@link NetworkInterfaceOptions}.
         *
         * @return created builder
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builder to build {@link NetworkInterfaceOptions}.
         */
        public static final class Builder {
            private String name;
            private boolean up = true;
            private boolean loopback = false;
            private boolean virtual = false;
            private String[] addresses = {};

            private Builder() {
            }

            public Builder withName(String name) {
                this.name = name;
                return this;
            }

            public Builder withUp(boolean up) {
                this.up = up;
                return this;
            }

            public Builder withLoopback(boolean loopback) {
                this.loopback = loopback;
                return this;
            }

            public Builder withVirtual(boolean virtual) {
                this.virtual = virtual;
                return this;
            }

            public Builder withAddresses(String... addresses) {
                this.addresses = addresses;
                return this;
            }

            public NetworkInterfaceOptions build() {
                return new NetworkInterfaceOptions(this);
            }
        }
    }
}
