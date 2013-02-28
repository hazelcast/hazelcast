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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class MapTransactionTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanUp() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTxnTimeout() {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newInstance(config);
        HazelcastInstance h2 = factory.newInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        Transaction txn = h1.getTransaction();
        txn.setTransactionTimeout(1);
        txn.begin();
        try {
            map1.put("1", "value");
            Thread.sleep(1010);
            map1.put("13", "value");
            fail();
        } catch (Throwable e) {
        }
        try {
            txn.commit();
            fail();
        } catch (IllegalStateException e) {
        }
        map2.lock("1");
        txn = h1.getTransaction();
        txn.setTransactionTimeout(1);
        txn.begin();
        try {
            map1.put("1", "value");
        } catch (Exception e) {
        }
    }

    @Test
    public void testTxnCommit() {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newInstance(config);
        HazelcastInstance h2 = factory.newInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        Transaction txn = h1.getTransaction();
        txn.begin();
        map1.put("1", "value");
        map1.put("13", "value");
        assertFalse(map2.tryPut("1", "value2", 0, null));
        assertEquals("value", map1.get("1"));
        assertEquals("value", map1.get("13"));
        assertNull(map2.get("1"));
        assertNull(map2.get("13"));
        txn.commit();
        assertEquals("value", map1.get("1"));
        assertEquals("value", map1.get("13"));
        assertEquals("value", map2.get("1"));
        assertEquals("value", map2.get("13"));
    }

    @Test
    public void testTxnRollback() {
        Config config = new Config();
        StaticNodeFactory factory = new StaticNodeFactory(2);
        HazelcastInstance h1 = factory.newInstance(config);
        HazelcastInstance h2 = factory.newInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        Transaction txn = h1.getTransaction();
        txn.begin();
        map1.put("1", "value");
        map1.put("13", "value");
        assertEquals("value", map1.get("1"));
        assertEquals("value", map1.get("13"));
        assertNull(map2.get("1"));
        assertNull(map2.get("13"));
        txn.rollback();
        assertNull(map1.get("1"));
        assertNull(map1.get("13"));
        assertNull(map2.get("1"));
        assertNull(map2.get("13"));
    }
}
