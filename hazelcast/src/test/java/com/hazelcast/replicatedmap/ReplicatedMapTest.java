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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapTest extends HazelcastTestSupport {

    @Test
    public void testAddObject() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map2.put("bar", "foo");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testUpdateObject() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.put("foo", "bar2");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        map2.put("foo", "bar3");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testRemoveObject() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.remove("foo");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertNull(value);
    }

    @Test
    public void testAddBinary() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map2.put("bar", "foo");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testUpdateBinary() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.put("foo", "bar2");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        map2.put("foo", "bar3");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testRemoveBinary() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.remove("foo");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertNull(value);
    }

}
