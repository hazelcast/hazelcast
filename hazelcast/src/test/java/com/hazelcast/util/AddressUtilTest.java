/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.AddressUtil.AddressMatcher;
import com.hazelcast.util.AddressUtil.InvalidAddressException;
import com.hazelcast.util.AddressUtil.Ip4AddressMatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import java.net.Inet6Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Collection;
import java.util.Vector;
import java.util.Enumeration;

import static com.hazelcast.util.AddressUtil.AddressHolder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AddressUtil class.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(HazelcastSerialClassRunner.class)
@PrepareForTest({Inet6Address.class, AddressUtil.class, NetworkInterface.class})
@Category(QuickTest.class)
public class AddressUtilTest extends HazelcastTestSupport {

    @Test
    public void testMatchAnyInterface() {
        assertTrue(AddressUtil.matchAnyInterface("10.235.194.23", asList("10.235.194.23", "10.235.193.121")));

        assertFalse(AddressUtil.matchAnyInterface("10.235.194.23", null));
        assertFalse(AddressUtil.matchAnyInterface("10.235.194.23", Collections.<String>emptyList()));
        assertFalse(AddressUtil.matchAnyInterface("10.235.194.23", singletonList("10.235.193.*")));
    }

    @Test
    public void testMatchInterface() {
        assertTrue(AddressUtil.matchInterface("fe80::62c5:0:fe05:480a%en0", "fe80::62c5:*:fe05:480a%en0"));
        assertTrue(AddressUtil.matchInterface("fe80::62c5:aefb:fe05:480a%en1", "fe80::62c5:0-ffff:fe05:480a"));
    }

    @Test
    public void testMatchInterface_whenInvalidInterface_thenReturnFalse() {
        assertFalse(AddressUtil.matchInterface("10.235.194.23", "bar"));
    }

    @Test
    public void testMatchAnyDomain() {
        assertTrue(AddressUtil.matchAnyDomain("hazelcast.com", singletonList("hazelcast.com")));

        assertFalse(AddressUtil.matchAnyDomain("hazelcast.com", null));
        assertFalse(AddressUtil.matchAnyDomain("hazelcast.com", Collections.<String>emptyList()));
        assertFalse(AddressUtil.matchAnyDomain("hazelcast.com", singletonList("abc.com")));
    }

    @Test
    public void testMatchDomain() {
        assertTrue(AddressUtil.matchDomain("hazelcast.com", "hazelcast.com"));
        assertTrue(AddressUtil.matchDomain("hazelcast.com", "*.com"));
        assertTrue(AddressUtil.matchDomain("jobs.hazelcast.com", "*.hazelcast.com"));
        assertTrue(AddressUtil.matchDomain("download.hazelcast.org", "*.hazelcast.*"));
        assertTrue(AddressUtil.matchDomain("download.hazelcast.org", "*.hazelcast.org"));

        assertFalse(AddressUtil.matchDomain("hazelcast.com", "abc.com"));
        assertFalse(AddressUtil.matchDomain("hazelcast.com", "*.hazelcast.com"));
        assertFalse(AddressUtil.matchDomain("hazelcast.com", "hazelcast.com.tr"));
        assertFalse(AddressUtil.matchDomain("hazelcast.com", "*.com.tr"));
        assertFalse(AddressUtil.matchDomain("www.hazelcast.com", "www.hazelcast.com.tr"));
    }

    @Test
    public void testParsingHostAndPort() {
        AddressHolder addressHolder = AddressUtil.getAddressHolder("[fe80::62c5:*:fe05:480a%en0]:8080");
        assertEquals("fe80::62c5:*:fe05:480a", addressHolder.getAddress());
        assertEquals(8080, addressHolder.getPort());
        assertEquals("en0", addressHolder.getScopeId());

        addressHolder = AddressUtil.getAddressHolder("[::ffff:192.0.2.128]:5700");
        assertEquals("::ffff:192.0.2.128", addressHolder.getAddress());
        assertEquals(5700, addressHolder.getPort());

        addressHolder = AddressUtil.getAddressHolder("192.168.1.1:5700");
        assertEquals("192.168.1.1", addressHolder.getAddress());
        assertEquals(5700, addressHolder.getPort());

        addressHolder = AddressUtil.getAddressHolder("hazelcast.com:80");
        assertEquals("hazelcast.com", addressHolder.getAddress());
        assertEquals(80, addressHolder.getPort());
    }

    @Test
    public void testIsIpAddress() {
        assertTrue(AddressUtil.isIpAddress("10.10.10.10"));
        assertTrue(AddressUtil.isIpAddress("111.12-66.123.*"));
        assertTrue(AddressUtil.isIpAddress("111-255.12-66.123.*"));
        assertTrue(AddressUtil.isIpAddress("255.255.123.*"));
        assertTrue(AddressUtil.isIpAddress("255.11-255.123.0"));
        assertFalse(AddressUtil.isIpAddress("255.11-256.123.0"));
        assertFalse(AddressUtil.isIpAddress("111.12-66-.123.*"));
        assertFalse(AddressUtil.isIpAddress("111.12*66-.123.-*"));
        assertFalse(AddressUtil.isIpAddress("as11d.897.hazelcast.com"));
        assertFalse(AddressUtil.isIpAddress("192.111.10.com"));
        assertFalse(AddressUtil.isIpAddress("192.111.10.999"));

        assertTrue(AddressUtil.isIpAddress("::1"));
        assertTrue(AddressUtil.isIpAddress("0:0:0:0:0:0:0:1"));
        assertTrue(AddressUtil.isIpAddress("2001:db8:85a3:0:0:8a2e:370:7334"));
        assertTrue(AddressUtil.isIpAddress("2001::370:7334"));
        assertTrue(AddressUtil.isIpAddress("fe80::62c5:0:fe05:480a%en0"));
        assertTrue(AddressUtil.isIpAddress("fe80::62c5:0:fe05:480a%en0"));
        assertTrue(AddressUtil.isIpAddress("2001:db8:85a3:*:0:8a2e:370:7334"));
        assertTrue(AddressUtil.isIpAddress("fe80::62c5:0-ffff:fe05:480a"));
        assertTrue(AddressUtil.isIpAddress("fe80::62c5:*:fe05:480a"));

        assertFalse(AddressUtil.isIpAddress("2001:acdb8:85a3:0:0:8a2e:370:7334"));
        assertFalse(AddressUtil.isIpAddress("2001::370::7334"));
        assertFalse(AddressUtil.isIpAddress("2001:370::7334.155"));
        assertFalse(AddressUtil.isIpAddress("2001:**:85a3:*:0:8a2e:370:7334"));
        assertFalse(AddressUtil.isIpAddress("fe80::62c5:0-ffff:fe05-:480a"));
        assertFalse(AddressUtil.isIpAddress("fe80::62c5:*:fe05-fffddd:480a"));
        assertFalse(AddressUtil.isIpAddress("fe80::62c5:*:fe05-ffxd:480a"));
    }

    @Test
    public void testAddressMatcher() {
        AddressMatcher address;
        address = AddressUtil.getAddressMatcher("fe80::62c5:*:fe05:480a%en0");
        assertTrue(address.isIPv6());
        assertEquals("fe80:0:0:0:62c5:*:fe05:480a", address.getAddress());

        address = AddressUtil.getAddressMatcher("192.168.1.1");
        assertTrue(address instanceof Ip4AddressMatcher);
        assertEquals("192.168.1.1", address.getAddress());

        address = AddressUtil.getAddressMatcher("::ffff:192.0.2.128");
        assertTrue(address.isIPv4());
        assertEquals("192.0.2.128", address.getAddress());
    }

    @Test
    public void testAddressMatcherFail() {
        try {
            AddressUtil.getAddressMatcher("fe80::62c5:47ff::fe05:480a%en0");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof InvalidAddressException);
        }
        try {
            AddressUtil.getAddressMatcher("fe80:62c5:47ff:fe05:480a%en0");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof InvalidAddressException);
        }
        try {
            AddressUtil.getAddressMatcher("[fe80:62c5:47ff:fe05:480a%en0");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof InvalidAddressException);
        }
        try {
            AddressUtil.getAddressMatcher("::ffff.192.0.2.128");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof InvalidAddressException);
        }
    }

    @Test
    public void testFixScopeIdAndGetInetAddress_whenNotLinkLocalAddress() throws SocketException, UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName("2001:db8:85a3:0:0:8a2e:370:7334");
        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inetAddress);
        assertEquals(inetAddress, actual);
    }

    @Test
    public void testFixScopeIdAndGetInetAddress_whenLinkLocalAddress() throws SocketException, UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8();

        Inet6Address inet6Address = PowerMockito.mock(Inet6Address.class);
        when(inet6Address.isLinkLocalAddress()).thenReturn(true);
        when(inet6Address.getScopeId()).thenReturn(1);
        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inet6Address);
        assertEquals(inet6Address, actual);
    }

    @Test
    public void testFixScopeIdAndGetInetAddress_whenLinkLocalAddress_withNoInterfaceBind() throws SocketException, UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8();

        Inet6Address inet6Address = PowerMockito.mock(Inet6Address.class);
        when(inet6Address.isLinkLocalAddress()).thenReturn(true);
        when(inet6Address.getScopeId()).thenReturn(0);
        when(inet6Address.getAddress()).thenReturn(null);
        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inet6Address);
        assertEquals(inet6Address, actual);
    }

    @Test
    public void testGetInetAddressFor() throws SocketException, UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8();

        InetAddress expected = InetAddress.getByName("2001:db8:85a3:0:0:8a2e:370:7334");
        Inet6Address inet6Address = PowerMockito.mock(Inet6Address.class);
        byte[] address = "address".getBytes();
        String scope = "1";

        PowerMockito.mockStatic(Inet6Address.class);

        when(inet6Address.getAddress()).thenReturn(address);
        when(inet6Address.isSiteLocalAddress()).thenReturn(true);
        when(Inet6Address.getByAddress(anyString(), eq(address), eq(Integer.parseInt(scope))))
                .thenReturn((Inet6Address) expected);

        InetAddress actual = AddressUtil.getInetAddressFor(inet6Address, scope);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetPossibleInetAddressesFor_whenNotLocalAddress() {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8();

        Inet6Address inet6Address = PowerMockito.mock(Inet6Address.class);
        when(inet6Address.isSiteLocalAddress()).thenReturn(false);
        when(inet6Address.isLinkLocalAddress()).thenReturn(false);
        Collection<Inet6Address> actual = AddressUtil.getPossibleInetAddressesFor(inet6Address);
        assertEquals(1, actual.size());
        assertTrue(actual.contains(inet6Address));
    }

    @Test
    public void testGetPossibleInetAddressesFor_whenLocalAddress() throws SocketException, UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8();

        InetAddress expected = InetAddress.getByName("2001:db8:85a3:0:0:8a2e:370:7334");
        Inet6Address inet6Address = PowerMockito.mock(Inet6Address.class);
        Inet6Address possibleAddress = PowerMockito.mock(Inet6Address.class);
        NetworkInterface networkInterface = PowerMockito.mock(NetworkInterface.class);

        Vector<NetworkInterface> networkInterfaces = new Vector<NetworkInterface>();
        networkInterfaces.add(networkInterface);
        Enumeration<NetworkInterface> networkInterfaceEnumeration = networkInterfaces.elements();

        Vector<InetAddress> inet6AddressVector = new Vector<InetAddress>();
        inet6AddressVector.add(possibleAddress);
        Enumeration<InetAddress> inetAddressEnumeration = inet6AddressVector.elements();

        PowerMockito.mockStatic(NetworkInterface.class);
        PowerMockito.mockStatic(Inet6Address.class);
        PowerMockito.when(NetworkInterface.getNetworkInterfaces()).thenReturn(networkInterfaceEnumeration);
        when(Inet6Address.getByAddress(anyString(), any(byte[].class), anyInt()))
                .thenReturn((Inet6Address) expected);
        when(networkInterface.getInetAddresses()).thenReturn(inetAddressEnumeration);
        when(possibleAddress.isLinkLocalAddress()).thenReturn(true);
        when(inet6Address.isSiteLocalAddress()).thenReturn(true);
        when(inet6Address.isLinkLocalAddress()).thenReturn(true);

        Collection<Inet6Address> actual = AddressUtil.getPossibleInetAddressesFor(inet6Address);
        assertEquals(1, actual.size());
        assertTrue(actual.contains(expected));
    }

    @Test
    public void testGetMatchingIpv4Addresses_whenWildcardForLastPart() {
        AddressMatcher addressMatcher = AddressUtil.getAddressMatcher("192.168.1.*");
        Collection<String> actual = AddressUtil.getMatchingIpv4Addresses(addressMatcher);
        assertEquals(256, actual.size());
    }

    @Test
    public void testGetMatchingIpv4Addresses_whenDashForLastPart() {
        AddressMatcher addressMatcher = AddressUtil.getAddressMatcher("192.168.1.1-42");
        Collection<String> actual = AddressUtil.getMatchingIpv4Addresses(addressMatcher);
        assertEquals(42, actual.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetMatchingIpv4Addresses_whenIPv6AsMatcher() {
        AddressMatcher addressMatcher = AddressUtil.getAddressMatcher("2001:db8:85a3:0:0:8a2e:370:7334");
        AddressUtil.getMatchingIpv4Addresses(addressMatcher);
    }
}

