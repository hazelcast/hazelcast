/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.WANReplicationQueueFullException;
import com.hazelcast.wan.impl.FullQueueWanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapWANExceptionTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server;

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance(getConfig());
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testMapPut() {
        IMap<Object, Object> map = client.getMap("wan-exception-client-test-map");
        map.put(1, 1);
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testMapPutAll() {
        IMap<Object, Object> map = client.getMap("wan-exception-client-test-map");
        Map<Object, Object> inputMap = MapUtil.createHashMap(1);
        inputMap.put(1, 1);
        map.putAll(inputMap);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName("dummyWan");

        wanConfig.addWanPublisherConfig(getWanPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("dummyWan");
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());

        config.addWanReplicationConfig(wanConfig);
        config.getMapConfig("default").setWanReplicationRef(wanRef);
        return config;
    }

    private WanPublisherConfig getWanPublisherConfig() {
        WanPublisherConfig target = new WanPublisherConfig();
        target.setClassName(FullQueueWanReplication.class.getName());
        return target;
    }
}

