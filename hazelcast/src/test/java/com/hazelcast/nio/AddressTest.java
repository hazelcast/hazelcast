/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeLocalhostResolvesTo_127_0_0_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AddressTest {

    @Test(expected = IllegalArgumentException.class)
    public void newAddress_InetSocketAddress_whenHostUnresolved() throws UnknownHostException {
        InetSocketAddress inetAddress = InetSocketAddress.createUnresolved("dontexist", 1);
        new Address(inetAddress);
    }

    @Test(expected = NullPointerException.class)
    public void newAddress_InetSocketAddress_whenNull() throws UnknownHostException {
        new Address((InetSocketAddress) null);
    }

    @Test
    public void testEqualsBasedOnSocksInetAddress() throws UnknownHostException {
        assumeLocalhostResolvesTo_127_0_0_1();
        Address address1 = new Address("127.0.0.1", 5701);
        Address address2 = new Address("localhost", 5701);
        Address address3 = new Address("localhost", 5702);
        Address address4 = new Address("foobarx", InetAddress.getByName("127.0.0.1"), 5702);
        Address address5 = new Address("member.hazelcast.com", InetAddress.getByName("10.0.0.1"), 5701);
        assertEquals(address1, address2);
        assertEquals(address3, address4);
        assertNotEquals(address1, address3);
        assertNotEquals(address2, address5);
        assertNotEquals(address3, address5);
    }
}
