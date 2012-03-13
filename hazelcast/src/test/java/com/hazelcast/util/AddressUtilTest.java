/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.util.AddressUtil.AddressMatcher;
import com.hazelcast.util.AddressUtil.InvalidAddressException;
import com.hazelcast.util.AddressUtil.Ip4AddressMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.util.AddressUtil.AddressHolder;

/**
 * Unit tests for AddressUtil class.
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class AddressUtilTest {

    @Test
    public void testParsingHostAndPort() {
        AddressHolder addressHolder = AddressUtil.getAddressHolder("[fe80::62c5:*:fe05:480a%en0]:8080");
        Assert.assertEquals("fe80::62c5:*:fe05:480a%en0", addressHolder.address);
        Assert.assertEquals(8080, addressHolder.port);

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
        Assert.assertEquals("en0", address.getScopeId());
        Assert.assertEquals("fe80:0:0:0:62c5:*:fe05:480a%en0", address.getAddress());

        address = AddressUtil.getAddressMatcher("192.168.1.1");
        Assert.assertTrue(address instanceof Ip4AddressMatcher);
        Assert.assertNull(address.getScopeId());
        Assert.assertEquals("192.168.1.1", address.getAddress());

        address = AddressUtil.getAddressMatcher("::ffff:192.0.2.128");
        Assert.assertTrue(address.isIPv4());
        Assert.assertNull(address.getScopeId());
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

}
