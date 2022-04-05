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
import com.hazelcast.instance.impl.DefaultAddressPicker.AddressDefinition;
import com.hazelcast.instance.impl.DefaultAddressPicker.InterfaceDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import static com.hazelcast.instance.impl.DefaultAddressPicker.PREFER_IPV4_STACK;
import static com.hazelcast.instance.impl.DefaultAddressPicker.PREFER_IPV6_ADDRESSES;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.internal.util.AddressUtil.getAddressHolder;
import static java.net.InetAddress.getByName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class DefaultAddressPickerTest {

    private static final String PUBLIC_HOST = "www.hazelcast.org";
    private static final String HAZELCAST_LOCAL_ADDRESS_PROP = "hazelcast.local.localAddress";

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-trace-default-address-picker.xml");

    @Rule
    public final OverridePropertyRule ruleSysPropHazelcastLocalAddress = clear(HAZELCAST_LOCAL_ADDRESS_PROP);
    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv4Stack = set(PREFER_IPV4_STACK, "false");
    @Rule
    public final OverridePropertyRule ruleSysPropPreferIpv6Addresses = clear(PREFER_IPV6_ADDRESSES);

    private ILogger logger = Logger.getLogger(AddressPicker.class);
    private Config config = new Config();
    private AddressPicker addressPicker;
    private InetAddress loopback;

    @Before
    public void setup() {
        InetAddress publicAddress = null;
        try {
            loopback = getByName("127.0.0.1");
            publicAddress = getByName(PUBLIC_HOST);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        assumeNotNull(loopback, publicAddress);
    }

    @After
    public void tearDown() {
        if (addressPicker != null) {
            IOUtil.closeResource(addressPicker.getServerSocketChannel(null));
        }
    }

    @Test
    public void testBindAddress_withDefaultPortAndLoopbackAddress() throws Exception {
        config.setProperty(HAZELCAST_LOCAL_ADDRESS_PROP, loopback.getHostAddress());
        testBindAddress(loopback);
    }

    @Test
    public void testBindAddress_withCustomPortAndLoopbackAddress() throws Exception {
        config.setProperty(HAZELCAST_LOCAL_ADDRESS_PROP, loopback.getHostAddress());
        int port = 6789;
        config.getNetworkConfig().setPort(port);
        testBindAddress(loopback);
    }

    @Test
    public void testBindAddress_withIPv4NonLoopbackAddressViaInterfaces() throws Exception {
        InetAddress address = findIPv4NonLoopbackInterface();
        assumeNotNull(address);

        config.getNetworkConfig().getInterfaces().setEnabled(true)
                .clear().addInterface(address.getHostAddress());

        testBindAddress(address);
    }

    @Test
    public void testBindAddress_withIPv6NonLoopbackAddressViaInterfaces() throws Exception {
        InetAddress address = findIPv6NonLoopbackInterface();
        assumeNotNull(address);

        config.setProperty(ClusterProperty.PREFER_IPv4_STACK.getName(), "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true)
                .clear().addInterface(address.getHostAddress());

        testBindAddress(address);
    }

    @Test
    public void testBindAddress_withIpv4NonLoopbackAddressViaTCPMembers() throws Exception {
        InetAddress address = findIPv4NonLoopbackInterface();
        assumeNotNull(address);

        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .clear().addMember(address.getHostAddress());

        testBindAddress(address);
    }

    @Test
    public void testBindAddress_withIPv6NonLoopbackAddressViaTCPMembers() throws Exception {
        InetAddress address = findIPv6NonLoopbackInterface();
        assumeNotNull(address);

        config.setProperty(ClusterProperty.PREFER_IPv4_STACK.getName(), "false");
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .clear().addMember(getHostAddress(address));

        testBindAddress(address);
    }

    @Test
    public void testBindAddress_withNonLoopbackAddressViaSystemProperty() throws Exception {
        InetAddress address = findAnyNonLoopbackInterface();
        assumeNotNull(address);

        config.setProperty(HAZELCAST_LOCAL_ADDRESS_PROP, getHostAddress(address));

        testBindAddress(address);
    }

    private void testBindAddress(InetAddress address) throws Exception {
        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();

        int port = config.getNetworkConfig().getPort();
        assertEquals(new Address(address, port), addressPicker.getBindAddress(null));
        assertEquals(addressPicker.getBindAddress(null), addressPicker.getPublicAddress(null));
    }

    @Test
    public void testBindAddress_withEphemeralPort() throws Exception {
        config.setProperty(HAZELCAST_LOCAL_ADDRESS_PROP, loopback.getHostAddress());
        config.getNetworkConfig().setPort(0);

        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();

        int port = addressPicker.getServerSocketChannel(null).socket().getLocalPort();

        assertEquals(new Address(loopback, port), addressPicker.getBindAddress(null));
        assertEquals(addressPicker.getBindAddress(null), addressPicker.getPublicAddress(null));
    }

    @Test
    public void testBindAddress_whenAddressAlreadyInUse() throws Exception {
        int port = 6789;
        config.getNetworkConfig().setPort(port);
        config.getNetworkConfig().setPortAutoIncrement(false);

        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();

        try {
            new DefaultAddressPicker(config, logger).pickAddress();
            fail("Should fail with 'java.net.BindException: Address already in use'");
        } catch (Exception expected) {
            // expected exception
        }
    }

    @Test
    public void testBindAddress_whenAddressAlreadyInUse_WithPortAutoIncrement() throws Exception {
        int port = 6789;
        config.getNetworkConfig().setPort(port);
        config.getNetworkConfig().setPortAutoIncrement(true);

        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();

        new DefaultAddressPicker(config, logger).pickAddress();
    }

    @Test
    public void testPublicAddress_withDefaultPortAndLoopbackAddress() throws Exception {
        testPublicAddress("127.0.0.1", -1);
    }

    @Test
    public void testPublicAddress_withDefaultPortAndLocalhost() throws Exception {
        testPublicAddress("localhost", -1);
    }

    @Test
    public void testPublicAddress_withSpecifiedHost() throws Exception {
        testPublicAddress(PUBLIC_HOST, -1);
    }

    @Test
    public void testPublicAddress_withSpecifiedHostAndPort() throws Exception {
        testPublicAddress(PUBLIC_HOST, 6789);
    }

    private void testPublicAddress(String host, int port) throws Exception {
        config.getNetworkConfig().setPublicAddress(port < 0 ? host : (host + ":" + port));
        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();

        if (port < 0) {
            port = config.getNetworkConfig().getPort();
        }

        assertEquals(new Address(host, port), addressPicker.getPublicAddress(null));
    }

    @Test
    public void testPublicAddress_withSpecifiedHostAndPortViaProperty() throws Exception {
        String host = PUBLIC_HOST;
        int port = 6789;
        config.setProperty("hazelcast.local.publicAddress", host + ":" + port);

        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();

        assertEquals(new Address(host, port), addressPicker.getPublicAddress(null));
    }

    @Test(expected = UnknownHostException.class)
    public void testPublicAddress_withInvalidAddress() throws Exception {
        config.getNetworkConfig().setPublicAddress("invalid");

        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();
    }

    @Test
    public void testBindAddress_withIPv6Address() throws Exception {
        assumeNotNull(findIPv6NonLoopbackInterface());

        System.setProperty(DefaultAddressPicker.PREFER_IPV4_STACK, "false");
        System.setProperty(DefaultAddressPicker.PREFER_IPV6_ADDRESSES, "true");
        config.setProperty(ClusterProperty.PREFER_IPv4_STACK.getName(), "false");

        addressPicker = new DefaultAddressPicker(config, logger);
        addressPicker.pickAddress();

        Address bindAddress = addressPicker.getBindAddress(null);
        assertTrue("Bind address: " + bindAddress, bindAddress.isIPv6());
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        InterfaceDefinition interfaceDefinition = new InterfaceDefinition("localhost", "127.0.0.1");
        InterfaceDefinition interfaceDefinitionSameAttributes = new InterfaceDefinition("localhost", "127.0.0.1");
        InterfaceDefinition interfaceDefinitionOtherHost = new InterfaceDefinition("otherHost", "127.0.0.1");
        InterfaceDefinition interfaceDefinitionOtherAddress = new InterfaceDefinition("localhost", "198.168.1.1");

        InetAddress otherInetAddress = getByName("198.168.1.1");
        AddressDefinition addressDefinition = new AddressDefinition("localhost", 5701, loopback);
        AddressDefinition addressDefinitionSameAttributes = new AddressDefinition("localhost", 5701, loopback);
        AddressDefinition addressDefinitionOtherHost = new AddressDefinition("otherHost", 5701, loopback);
        AddressDefinition addressDefinitionOtherPort = new AddressDefinition("localhost", 5702, loopback);
        AddressDefinition addressDefinitionOtherInetAddress = new AddressDefinition("localhost", 5701, otherInetAddress);

        // InterfaceDefinition.equals()
        assertEquals(interfaceDefinition, interfaceDefinition);
        assertEquals(interfaceDefinition, interfaceDefinitionSameAttributes);

        assertNotEquals(interfaceDefinition, null);
        assertNotEquals(interfaceDefinition, new Object());

        assertNotEquals(interfaceDefinition, interfaceDefinitionOtherHost);
        assertNotEquals(interfaceDefinition, interfaceDefinitionOtherAddress);

        // InterfaceDefinition.hashCode()
        assertEquals(interfaceDefinition.hashCode(), interfaceDefinition.hashCode());
        assertEquals(interfaceDefinition.hashCode(), interfaceDefinitionSameAttributes.hashCode());

        assertNotEquals(interfaceDefinition.hashCode(), interfaceDefinitionOtherHost.hashCode());
        assertNotEquals(interfaceDefinition.hashCode(), interfaceDefinitionOtherAddress.hashCode());

        // AddressDefinition.equals()
        assertEquals(addressDefinition, addressDefinition);
        assertEquals(addressDefinition, addressDefinitionSameAttributes);

        assertNotEquals(addressDefinition, null);
        assertNotEquals(addressDefinition, new Object());

        assertNotEquals(addressDefinition, addressDefinitionOtherHost);
        assertNotEquals(addressDefinition, addressDefinitionOtherPort);
        assertNotEquals(addressDefinition, addressDefinitionOtherInetAddress);

        // AddressDefinition.hashCode()
        assertEquals(addressDefinition.hashCode(), addressDefinition.hashCode());
        assertEquals(addressDefinition.hashCode(), addressDefinitionSameAttributes.hashCode());

        assertNotEquals(addressDefinition.hashCode(), addressDefinitionOtherHost.hashCode());
        assertNotEquals(addressDefinition.hashCode(), addressDefinitionOtherPort.hashCode());
        assertNotEquals(addressDefinition.hashCode(), addressDefinitionOtherInetAddress.hashCode());
    }

    private static InetAddress findAnyNonLoopbackInterface() {
        return findNonLoopbackInterface(false, false);
    }

    static InetAddress findIPv4NonLoopbackInterface() {
        return findNonLoopbackInterface(true, false);
    }

    static InetAddress findIPv6NonLoopbackInterface() {
        System.setProperty(DefaultAddressPicker.PREFER_IPV6_ADDRESSES, "true");
        return findNonLoopbackInterface(false, true);
    }

    private static InetAddress findNonLoopbackInterface(boolean enforceIPv4, boolean enforceIPv6) {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isLoopbackAddress()) {
                        continue;
                    }
                    if (enforceIPv4 && address instanceof Inet6Address) {
                        continue;
                    }
                    if (enforceIPv6 && address instanceof Inet4Address) {
                        continue;
                    }
                    return address;
                }
            }

        } catch (SocketException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getHostAddress(InetAddress address) {
        return getAddressHolder(address.getHostAddress()).getAddress();
    }
}
