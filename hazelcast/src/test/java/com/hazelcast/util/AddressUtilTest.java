/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.util.AddressUtil.AddressMatcher;
import com.hazelcast.util.AddressUtil.InvalidAddressException;
import com.hazelcast.util.AddressUtil.Ip4AddressMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.util.AddressUtil.AddressHolder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for AddressUtil class.
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class AddressUtilTest {

    @Test
    public void testParsingHostAndPort() {
        AddressHolder addressHolder = AddressUtil.getAddressHolder("[fe80::62c5:*:fe05:480a%en0]:8080");
        Assert.assertEquals("fe80::62c5:*:fe05:480a", addressHolder.address);
        Assert.assertEquals(8080, addressHolder.port);
        Assert.assertEquals("en0", addressHolder.scopeId);

        addressHolder = AddressUtil.getAddressHolder("[::ffff:192.0.2.128]:5700");
        Assert.assertEquals("::ffff:192.0.2.128", addressHolder.address);
        Assert.assertEquals(5700, addressHolder.port);

        addressHolder = AddressUtil.getAddressHolder("192.168.1.1:5700");
        Assert.assertEquals("192.168.1.1", addressHolder.address);
        Assert.assertEquals(5700, addressHolder.port);

        addressHolder = AddressUtil.getAddressHolder("hazelcast.com:80");
        Assert.assertEquals("hazelcast.com", addressHolder.address);
        Assert.assertEquals(80, addressHolder.port);
    }

    @Test
    public void testAddressMatcher() {
        AddressMatcher address;
        address = AddressUtil.getAddressMatcher("fe80::62c5:*:fe05:480a%en0");
        Assert.assertTrue(address.isIPv6());
        Assert.assertEquals("fe80:0:0:0:62c5:*:fe05:480a", address.getAddress());

        address = AddressUtil.getAddressMatcher("192.168.1.1");
        Assert.assertTrue(address instanceof Ip4AddressMatcher);
        Assert.assertEquals("192.168.1.1", address.getAddress());

        address = AddressUtil.getAddressMatcher("::ffff:192.0.2.128");
        Assert.assertTrue(address.isIPv4());
        Assert.assertEquals("192.0.2.128", address.getAddress());
    }

    @Test
    public void testAddressMatcherFail() {
        try {
            AddressUtil.getAddressMatcher("fe80::62c5:47ff::fe05:480a%en0");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidAddressException);
        }
        try {
            AddressUtil.getAddressMatcher("fe80:62c5:47ff:fe05:480a%en0");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidAddressException);
        }
        try {
            AddressUtil.getAddressMatcher("[fe80:62c5:47ff:fe05:480a%en0");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidAddressException);
        }
        try {
            AddressUtil.getAddressMatcher("::ffff.192.0.2.128");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof InvalidAddressException);
        }
    }

    @Test
    public void testAddressMatching() {
        Assert.assertTrue(AddressUtil.matchInterface("fe80::62c5:0:fe05:480a%en0", "fe80::62c5:*:fe05:480a%en0"));
        Assert.assertTrue(AddressUtil.matchInterface("fe80::62c5:aefb:fe05:480a%en1", "fe80::62c5:0-ffff:fe05:480a"));
    }

    @Test
    public void matchAddress() {
        assertTrue(AddressUtil.matchAnyInterface("10.235.194.23", Arrays.asList("10.235.194.23", "10.235.193.121")));
    }

    @Test
    public void doNotMatchAddress() {
        assertFalse(AddressUtil.matchAnyInterface("10.235.194.23", Arrays.asList("10.235.193.*")));
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

}
