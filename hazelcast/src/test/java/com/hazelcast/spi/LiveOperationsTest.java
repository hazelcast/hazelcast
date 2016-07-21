/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LiveOperationsTest {

    private Address address;
    private Address anotherAddress;

    private LiveOperations liveOperations;

    @Before
    public void setUp() throws Exception {
        address = new Address("127.0.0.1", 5701);
        anotherAddress = new Address("127.0.0.1", 5702);

        liveOperations = new LiveOperations(address);
    }

    @Test
    public void testAdd() {
        liveOperations.add(anotherAddress, 23);

        Set<Address> addresses = liveOperations.addresses();
        assertEquals(1, addresses.size());
        assertEquals(anotherAddress, addresses.iterator().next());
    }

    @Test
    public void testAdd_whenAddressIsNull_thenAddsLocalAddress() {
        liveOperations.add(null, 42);

        Set<Address> addresses = liveOperations.addresses();
        assertEquals(1, addresses.size());
        assertEquals(address, addresses.iterator().next());
    }

    @Test
    public void testAdd_whenCallIdIsZero_thenNoAddressIsAdded() {
        liveOperations.add(anotherAddress, 0);

        Set<Address> addresses = liveOperations.addresses();
        assertEquals(0, addresses.size());
    }

    @Test
    public void testCallIds() {
        liveOperations.add(anotherAddress, 23);

        long[] callIds = liveOperations.callIds(anotherAddress);

        assertEquals(1, callIds.length);
        assertEquals(23, callIds[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCallIds_whenAddressIsUnknown_thenThrowException() {
        liveOperations.callIds(address);
    }

    @Test
    public void testClearAndInitMember() {
        liveOperations.add(address, 23);

        liveOperations.clear();

        liveOperations.initMember(anotherAddress);

        long[] callIds = liveOperations.callIds(anotherAddress);
        assertEquals(0, callIds.length);
    }
}
