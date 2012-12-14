/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.Transaction;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClusterListTest {
    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testIndex() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final IList l1 = h1.getList("default");
        final IList l2 = h2.getList("default");
        for (int i = 0; i < 1000; i++) {
            l1.add(i);
        }
        for (int i = 0; i < 1000; i++) {
            l2.add(i);
        }
        for (int i = 0; i < 1000; i++) {
            l2.add(i);
        }
        assertEquals(3000, l1.size());
        assertEquals(3000, l2.size());
        assertEquals(5, l1.indexOf(5));
        assertEquals(5, l2.indexOf(5));
        assertEquals(2005, l1.lastIndexOf(5));
        assertEquals(2005, l2.lastIndexOf(5));
        assertEquals(5, l1.get(5));
        assertEquals(5, l2.get(5));
        assertEquals(5, l1.get(2005));
        assertEquals(5, l2.get(2005));
        l2.add(2, "item2");
        l2.add(2000, "item2000");
        assertEquals(3002, l1.size());
        assertEquals(3002, l2.size());
        assertEquals("item2", l2.get(2));
        assertEquals("item2000", l2.get(2000));
        assertEquals("item2", l2.set(2, "newitem2"));
        assertEquals("item2000", l2.set(2000, "newitem2000"));
        assertEquals("newitem2", l2.get(2));
        assertEquals("newitem2000", l2.get(2000));
        try {
            assertEquals("item2", l2.get(-1));
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            assertEquals("item2", l2.set(10000, "avalue"));
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        try {
            l2.get(10000);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }
        assertEquals(3002, l1.size());
        assertEquals(3002, l2.size());
        assertEquals("newitem2000", l2.remove(2000));
        assertEquals("newitem2", l2.remove(2));
        assertEquals(3000, l1.size());
        assertEquals(3000, l2.size());
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, l2.remove(0));
        }
        assertEquals(2000, l1.size());
        assertEquals(2000, l2.size());
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, l2.remove(0));
        }
        assertEquals(1000, l1.size());
        assertEquals(1000, l2.size());
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, l2.remove(0));
        }
        assertEquals(0, l1.size());
        assertEquals(0, l2.size());
    }

    // issue: List is not keeping the order in transactional context
    @Test
    public void testIssue73() throws Exception {
        IList list = Hazelcast.getList("default");
        list.clear();

        Transaction txn1 = Hazelcast.getTransaction();
        txn1.begin();
        list.add("1");
        list.add("2");
        list.add("4");
        txn1.commit();

        assertEquals(3, list.size());
        assertEquals("1", list.get(0));
        assertEquals("2", list.get(1));
        assertEquals("4", list.get(2));

        Transaction txn2 = Hazelcast.getTransaction();
        txn2.begin();
        list.add(0, "0");
        list.add(3, "3");
        txn2.commit();

        assertEquals(5, list.size());
        assertEquals("0", list.get(0));
        assertEquals("1", list.get(1));
        assertEquals("2", list.get(2));
        assertEquals("3", list.get(3));
        assertEquals("4", list.get(4));
    }


}
