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

package com.hazelcast.collection.impl.list;

import com.google.common.collect.Iterators;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientListProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientListTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private ClientListProxy<Integer> list;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance(getConfig());

        list = (ClientListProxy) hazelcastFactory.newHazelcastClient(new ClientConfig())
                                                 .getList(randomName());
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testDataIterator() {
        list.add(1);
        list.add(2);

        assertEquals(2, Iterators.size(list.dataIterator()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDataIterator_throwsException_whenRemove() {
        list.add(1);

        Iterator<Data> iterator = list.dataIterator();

        iterator.next();
        iterator.remove();
    }

    @Test
    public void testDataSublist() {
        list.add(1);
        list.add(2);
        list.add(3);

        List<Data> listTest = list.dataSubList(1, 2);

        assertEquals(1, listTest.size());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testDataSublist_whenFromIndexIllegal() {
        list.subList(8, 7);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testDataSublist_whenToIndexIllegal() {
        list.add(1);
        list.add(2);

        list.subList(1, 3);
    }
}
