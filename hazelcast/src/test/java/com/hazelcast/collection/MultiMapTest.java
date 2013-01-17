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

package com.hazelcast.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.GroupProperties;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @ali 1/17/13
 */
public class MultiMapTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        Hazelcast.shutdownAll();
    }

    @Test
    public void test1() throws InterruptedException {
        Config c = new Config();
        c.getMultiMapConfig("mm").setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c);

        MultiMap<String, String> mm1 = h1.getMultiMap("mm");
        MultiMap<String, String> mm2 = h2.getMultiMap("mm");
        MultiMap<String, String> mm3 = h3.getMultiMap("mm");

        for (int i=0; i<10; i++){
            mm1.put("key"+i,"value1_"+i);
            mm2.put("key"+i,"value2_"+i);
            mm3.put("key"+i,"value3_"+i);
        }
        mm1.remove("key4","value1_4");
        mm2.remove("key4","value2_4");
        mm3.remove("key4","value4_4");

        assertEquals(28, mm1.size());
        assertEquals(28, mm2.size());
        assertEquals(28, mm3.size());
        assertEquals(1, mm2.valueCount("key4"));

    }
}
