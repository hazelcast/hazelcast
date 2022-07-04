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

package com.hazelcast.client.projection;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.MapProjectionTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapProjectionTest extends MapProjectionTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Override
    public <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(InMemoryFormat.OBJECT);

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "3")
                .addMapConfig(mapConfig);

        factory.newInstances(config, nodeCount);
        instance0 = factory.newHazelcastClient();

        return instance0.getMap("aggr");
    }

    @Override
    public void projection_1Node_objectValue_withPartitionSet() {
        // ignore the test on client - not implemented on client
    }

    @Override
    public void projection_3Nodes_objectValue_withPartitionSet() {
        // ignore the test on client - not implemented on client
    }
}
