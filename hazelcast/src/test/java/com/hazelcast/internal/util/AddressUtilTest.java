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

package com.hazelcast.internal.util;

import com.hazelcast.internal.util.AddressUtil.AddressMatcher;
import com.hazelcast.internal.util.AddressUtil.InvalidAddressException;
import com.hazelcast.internal.util.AddressUtil.Ip4AddressMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.internal.util.AddressUtil.AddressHolder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for AddressUtil class.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AddressUtilTest extends HazelcastTestSupport {

    private static final String SOME_NOT_LOCAL_ADDRESS = "2001:db8:85a3:0:0:8a2e:370:7334";
    private static final String SOME_LINK_LOCAL_ADDRESS = "fe80::85a3:0:0:8a2e:370:7334";
    private static final String SOME_SITE_LOCAL_ADDRESS = "fec0::85a3:0:0:8a2e:370:7334";
    private static final String SOME_OTHER_LINK_LOCAL_ADDRESS = "fe80::85a3:0:0:8a2e:370:7777";

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
        assumeThatJDK8OrHigher();

        byte[] address = InetAddress.getByName(SOME_LINK_LOCAL_ADDRESS).getAddress();
        Inet6Address inet6Address = Inet6Address.getByAddress(SOME_LINK_LOCAL_ADDRESS, address, 1);
        assertThat(inet6Address.isLinkLocalAddress()).isTrue();

        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inet6Address);

        assertEquals(inet6Address, actual);
    }

    @Test
    public void testFixScopeIdAndGetInetAddress_whenLinkLocalAddress_withNoInterfaceBind() throws SocketException, UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8OrHigher();
        Inet6Address inet6Address = createInet6AddressWithScope(SOME_LINK_LOCAL_ADDRESS, 0);
        assertThat(inet6Address.isLinkLocalAddress()).isTrue();

        InetAddress actual = AddressUtil.fixScopeIdAndGetInetAddress(inet6Address);

        assertEquals(inet6Address, actual);
    }

    private Inet6Address createInet6AddressWithScope(String address, int scopeId) throws UnknownHostException {
        byte[] rawAddress = InetAddress.getByName(address).getAddress();
        return Inet6Address.getByAddress(address, rawAddress, scopeId);
    }

    @Test
    public void testGetInetAddressFor() throws SocketException, UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8OrHigher();
        Inet6Address inet6Address = createInet6AddressWithScope(SOME_SITE_LOCAL_ADDRESS, 1);
        assertThat(inet6Address.isSiteLocalAddress()).isTrue();

        InetAddress actual = AddressUtil.getInetAddressFor(inet6Address, "1");

        assertEquals(inet6Address, actual);
    }

    @Test
    public void testGetPossibleInetAddressesFor_whenNotLocalAddress() throws UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8OrHigher();

        Inet6Address inet6Address = (Inet6Address) Inet6Address.getByName(SOME_NOT_LOCAL_ADDRESS);
        assertThat(inet6Address.isSiteLocalAddress()).isFalse();
        assertThat(inet6Address.isLinkLocalAddress()).isFalse();

        Collection<Inet6Address> actual = AddressUtil.getPossibleInetAddressesFor(inet6Address);
        assertEquals(1, actual.size());
        assertTrue(actual.contains(inet6Address));
    }

    @Test
    public void testGetPossibleInetAddressesFor_whenLocalAddress() throws SocketException, UnknownHostException {
        // refer to https://github.com/hazelcast/hazelcast/pull/13069#issuecomment-388719847
        assumeThatJDK8OrHigher();

        Inet6Address inet6Address = (Inet6Address) Inet6Address.getByName(SOME_LINK_LOCAL_ADDRESS);
        assertThat(inet6Address.isLinkLocalAddress()).isTrue();

        Inet6Address possibleAddress = (Inet6Address) Inet6Address.getByName(SOME_OTHER_LINK_LOCAL_ADDRESS);

        NetworkInterfaceInfo networkInterface = NetworkInterfaceInfo.builder("eth1").withInetAddresses(possibleAddress).build();
        AddressUtil.setNetworkInterfacesEnumerator(new DummyNetworkInterfacesEnumerator(networkInterface));

        Collection<Inet6Address> actual = AddressUtil.getPossibleInetAddressesFor(inet6Address);
        assertEquals(1, actual.size());
        assertTrue(actual.contains(inet6Address));
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

