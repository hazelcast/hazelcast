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
package com.hazelcast.map.impl.operation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapChunkTest extends HazelcastTestSupport {

    @Test
    public void smoke() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        IMap test = node1.getMap("test");
        for (int i = 0; i < 1_000; i++) {
            test.set(i, i);
        }
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        node1.shutdown();

        assertEquals(1_000, node2.getMap("test").size());
    }

    @Test
    public void smoke_multiple_map() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        for (int j = 0; j < 10; j++) {
            IMap test = node1.getMap("test-" + j);
            for (int i = 0; i < 1_000; i++) {
                test.set(i, i);
            }
        }
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        node1.shutdown();

        for (int j = 0; j < 10; j++) {
            IMap test = node2.getMap("test-" + j);
            assertEquals(1_000, test.size());
        }
    }
}
