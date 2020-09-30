/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class MapDisableCopyOnReadTest extends HazelcastTestSupport {

    private MapProxyImpl<String, TestEntityImmutable> immutableMapProxy;
    private MapProxyImpl<String, TestEntity> mutableMapProxy;
    private PartitionService partitionService;

    @Before
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setup() {
        Config config = smallInstanceConfig()
                .addMapConfig(new MapConfig("myMap*").setInMemoryFormat(InMemoryFormat.OBJECT));
        HazelcastInstance instance = createHazelcastInstance(config);
        partitionService = instance.getPartitionService();
        immutableMapProxy = (MapProxyImpl) instance.getMap("myMapImmutable");
        mutableMapProxy = (MapProxyImpl) instance.getMap("myMapMutable");
    }

    @Test
    public void testGetImmutable() {
        TestEntityImmutable tc = new TestEntityImmutable("hello");
        immutableMapProxy.put("testCor", tc);
        TestEntityImmutable result1 = immutableMapProxy.get("testCor");
        TestEntityImmutable result2 = immutableMapProxy.get("testCor");
        assertSame(result1, result2);
    }

    @Test
    public void testGetMutable() {
        TestEntity tc = new TestEntity("hello");
        mutableMapProxy.put("testCor", tc);
        TestEntity result1 = mutableMapProxy.get("testCor");
        TestEntity result2 = mutableMapProxy.get("testCor");
        assertNotSame(result1, result2);
    }

    @Test
    public void testMapIteratorImmutable() {
        String key = "testCorKey";
        TestEntityImmutable tc = new TestEntityImmutable("hello");
        immutableMapProxy.put(key, tc);
        int partitionId = partitionService.getPartition(key).getPartitionId();

        TestEntityImmutable result1 =  immutableMapProxy.iterator(10, partitionId, false).next().getValue();
        TestEntityImmutable result2 =  immutableMapProxy.iterator(10, partitionId, false).next().getValue();
        assertSame(result1, result2);
    }

    @Test
    public void testMapIteratorForMutable() {
        String key = "testCorKey";
        TestEntity tc = new TestEntity("hello");
        mutableMapProxy.put(key, tc);
        int partitionId = partitionService.getPartition(key).getPartitionId();

        TestEntity result1 =  mutableMapProxy.iterator(10, partitionId, false).next().getValue();
        TestEntity result2 =  mutableMapProxy.iterator(10, partitionId, false).next().getValue();
        assertNotSame(result1, result2);
    }

    @Test
    public void testClassCastException() {
        String key = "testCorKey";
        TestEntityImmutable tc = new TestEntityImmutable("hello");
        immutableMapProxy.put(key, tc);
        int partitionId = partitionService.getPartition(key).getPartitionId();

        TestEntityImmutable result1 =  immutableMapProxy.iterator(10, partitionId, false).next().getValue();
        TestEntityImmutable result2 =  immutableMapProxy.iterator(10, partitionId, false).next().getValue();
        assertSame(result1, result2);
    }
}
