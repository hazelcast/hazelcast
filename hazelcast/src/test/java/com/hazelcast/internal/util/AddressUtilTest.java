/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.util.AddressUtil.AddressHolder;
import com.hazelcast.internal.util.AddressUtil.AddressMatcher;
import com.hazelcast.internal.util.AddressUtil.InvalidAddressException;
import com.hazelcast.internal.util.AddressUtil.Ip4AddressMatcher;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;

import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for AddressUtil class.
 */
@QuickTest
class AddressUtilTest {

    private static final String SOME_NOT_LOCAL_ADDRESS = "2001:db8:85a3:0:0:8a2e:370:7334";
    private static final String SOME_LINK_LOCAL_ADDRESS = "fe80::85a3:0:0:8a2e:370:7334";
    private static final String SOME_SITE_LOCAL_ADDRESS = "fec0::85a3:0:0:8a2e:370:7334";
    private static final String SOME_OTHER_LINK_LOCAL_ADDRESS = "fe80::85a3:0:0:8a2e:370:7777";

    @Test
    void testMatchAnyInterface() {
        assertTrue(AddressUtil.matchAnyInterface("10.235.194.23", of("10.235.194.23", "10.235.193.121")));

        assertFalse(AddressUtil.matchAnyInterface("10.235.194.23", null));
        assertFalse(AddressUtil.matchAnyInterface("10.235.194.23", Collections.emptyList()));
        assertFalse(AddressUtil.matchAnyInterface("10.235.194.23", of("10.235.193.*")));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', useHeadersInDisplayName = true, textBlock = """
                Address                         | Pattern
                fe80::62c5:0:fe05:480a%en0      | fe80::62c5:*:fe05:480a%en0
                fe80::62c5:aefb:fe05:480a%en1   | fe80::62c5:0-ffff:fe05:480a
            """)
    void testMatchInterface(String address, String pattern) {
        assertTrue(AddressUtil.matchInterface(address, pattern));
    }

    @Test
    void testMatchInterface_whenInvalidInterface_thenReturnFalse() {
        assertFalse(AddressUtil.matchInterface("10.235.194.23", "bar"));
    }

    @Test
    void testMatchAnyDomain() {
        assertTrue(AddressUtil.matchAnyDomain("hazelcast.com", of("hazelcast.com")));

        assertFalse(AddressUtil.matchAnyDomain("hazelcast.com", null));
        assertFalse(AddressUtil.matchAnyDomain("hazelcast.com", Collections.emptyList()));
        assertFalse(AddressUtil.matchAnyDomain("hazelcast.com", of("abc.com")));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', useHeadersInDisplayName = true, textBlock = """
            Name                    | Address               | Expected Match Domain?
            hazelcast.com           | hazelcast.com         | true
            hazelcast.com           | *.com                 | true
            jobs.hazelcast.com      | *.hazelcast.com       | true
            download.hazelcast.org  | *.hazelcast.*         | true
            download.hazelcast.org  | *.hazelcast.org       | true
            hazelcast.com           | abc.com               | false
            hazelcast.com           | *.hazelcast.com       | false
            hazelcast.com           | hazelcast.com.tr      | false
            hazelcast.com           | *.com.tr              | false
            hazelcast.com           | www.hazelcast.com.tr  | false
            """)
    void testMatchDomain(String name, String address, boolean expectedMatchDomain) {
        assertEquals(expectedMatchDomain, AddressUtil.matchDomain(name, address));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', useHeadersInDisplayName = true, textBlock = """
            Address                             | Expected Address          | Expected Port | Expected Scope ID
            [fe80::62c5:*:fe05:480a%en0]:8080   | fe80::62c5:*:fe05:480a    | 8080          | en0
            [::ffff:192.0.2.128]:5700           | ::ffff:192.0.2.128        | 5700          |
            192.168.1.1:5700                    | 192.168.1.1               | 5700          |
            hazelcast.com:80                    | hazelcast.com             | 80            |
            """)
    void testParsingHostAndPort(String address, String expectedAddress, int expectedPort, String expectedScopeId) {
        AddressHolder addressHolder = AddressUtil.getAddressHolder(address);
        assertEquals(expectedAddress, addressHolder.getAddress());
        assertEquals(expectedPort, addressHolder.getPort());
        assertEquals(expectedScopeId, addressHolder.getScopeId());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', useHeadersInDisplayName = true, textBlock = """
            Address                             | Is IP Address?
            # IPv4
            # true
            10.10.10.10                         | true
            111.12-66.123.*                     | true
            111-255.12-66.123.*                 | true
            255.255.123.*                       | true
            255.11-255.123.0                    | true
            # false
            255.11-256.123.0                    | false
            111.12-66-.123.*                    | false
            111.12*66-.123.-*                   | false
            as11d.897.hazelcast.com             | false
            192.111.10.com                      | false
            192.111.10.999                      | false
            # IPv6
            # true
            ::1                                 | true
            0:0:0:0:0:0:0:1                     | true
            2001:db8:85a3:0:0:8a2e:370:7334     | true
            2001::370:7334                      | true
            fe80::62c5:0:fe05:480a%en0          | true
            2001:db8:85a3:*:0:8a2e:370:7334     | true
            fe80::62c5:0-ffff:fe05:480a         | true
            fe80::62c5:*:fe05:480a              | true
            # false
            2001:acdb8:85a3:0:0:8a2e:370:7334   | false
            2001::370::7334                     | false
            2001:370::7334.155                  | false
            2001:**:85a3:*:0:8a2e:370:7334      | false
            fe80::62c5:0-ffff:fe05-:480a        | false
            fe80::62c5:*:fe05-fffddd:480a       | false
            fe80::62c5:*:fe05-ffxd:480a         | false
            """)
    void testIsIpAddress(String address, boolean isIPAdress) {
        assertEquals(isIPAdress, AddressUtil.isIpAddress(address));
    }

    @Test
    void testAddressMatcher() {
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

    @ParameterizedTest
    @ValueSource(strings = {"fe80::62c5:47ff::fe05:480a%en0", "fe80:62c5:47ff:fe05:480a%en0", "[fe80:62c5:47ff:fe05:480a%en0",
            "::ffff.192.0.2.128"})
    void testAddressMatcherFail(String address) {
        assertThrows(InvalidAddressException.class, () -> AddressUtil.getAddressMatcher(address));
    }

    @Test
    void testFixScopeIdAndGetInetAddress_whenNotLinkLocalAddress() throws SocketException, UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName("2001:db8:85a3:0:0:8a2e:370:7334");
        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inetAddress);
        assertEquals(inetAddress, actual);
    }

    @Test
    void testFixScopeIdAndGetInetAddress_whenLinkLocalAddress() throws SocketException, UnknownHostException {
        byte[] address = InetAddress.getByName(SOME_LINK_LOCAL_ADDRESS).getAddress();
        Inet6Address inet6Address = Inet6Address.getByAddress(SOME_LINK_LOCAL_ADDRESS, address, 1);
        assertTrue(inet6Address.isLinkLocalAddress());

        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inet6Address);

        assertEquals(inet6Address, actual);
    }

    @Test
    void testFixScopeIdAndGetInetAddress_whenLinkLocalAddress_withNoInterfaceBind()
            throws SocketException, UnknownHostException {
        Inet6Address inet6Address = createInet6AddressWithScope(SOME_LINK_LOCAL_ADDRESS, 0);
        assertTrue(inet6Address.isLinkLocalAddress());

        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inet6Address);

        assertEquals(inet6Address, actual);
    }

    private static Inet6Address createInet6AddressWithScope(String address, int scopeId) throws UnknownHostException {
        byte[] rawAddress = InetAddress.getByName(address).getAddress();
        return Inet6Address.getByAddress(address, rawAddress, scopeId);
    }

    @Test
    void testGetInetAddressFor() throws SocketException, UnknownHostException {
        Inet6Address inet6Address = createInet6AddressWithScope(SOME_SITE_LOCAL_ADDRESS, 1);
        assertTrue(inet6Address.isSiteLocalAddress());

        InetAddress actual = AddressUtil.getInetAddressFor(inet6Address, "1");

        assertEquals(inet6Address, actual);
    }

    @Test
    void testGetPossibleInetAddressesFor_whenNotLocalAddress() throws UnknownHostException {
        Inet6Address inet6Address = (Inet6Address) InetAddress.getByName(SOME_NOT_LOCAL_ADDRESS);
        assertFalse(inet6Address.isSiteLocalAddress());
        assertFalse(inet6Address.isLinkLocalAddress());

        Collection<Inet6Address> actual = AddressUtil.getPossibleInetAddressesFor(inet6Address);
        assertEquals(1, actual.size());
        assertTrue(actual.contains(inet6Address));
    }

    @Test
    void testGetPossibleInetAddressesFor_whenLocalAddress() throws UnknownHostException {
        Inet6Address inet6Address = (Inet6Address) InetAddress.getByName(SOME_LINK_LOCAL_ADDRESS);
        assertTrue(inet6Address.isLinkLocalAddress());

        Inet6Address possibleAddress = (Inet6Address) InetAddress.getByName(SOME_OTHER_LINK_LOCAL_ADDRESS);

        NetworkInterfaceInfo networkInterface = NetworkInterfaceInfo.builder("eth1").withInetAddresses(possibleAddress).build();
        AddressUtil.setNetworkInterfacesEnumerator(new DummyNetworkInterfacesEnumerator(networkInterface));

        Collection<Inet6Address> actual = AddressUtil.getPossibleInetAddressesFor(inet6Address);
        assertEquals(1, actual.size());
        assertTrue(actual.contains(inet6Address));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', useHeadersInDisplayName = true, textBlock = """
            Description             | Address           | Size
            Wildcard For Last Part  | 192.168.1.*       | 256
            Dash For Last Part      | 192.168.1.1-42    | 42
            """)
    void testGetMatchingIpv4Addresses(@SuppressWarnings("unused") String description, String address, int size) {
        AddressMatcher addressMatcher = AddressUtil.getAddressMatcher(address);
        Collection<String> actual = AddressUtil.getMatchingIpv4Addresses(addressMatcher);
        assertEquals(size, actual.size());
    }

    @Test
    void testGetMatchingIpv4Addresses_whenIPv6AsMatcher() {
        AddressMatcher addressMatcher = AddressUtil.getAddressMatcher("2001:db8:85a3:0:0:8a2e:370:7334");
        assertThrows(IllegalArgumentException.class, () -> AddressUtil.getMatchingIpv4Addresses(addressMatcher));
    }
}

