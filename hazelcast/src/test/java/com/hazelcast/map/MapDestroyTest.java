/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import groovy.lang.Category;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapDestroyTest extends HazelcastTestSupport {

    private HazelcastInstance localInstance;
    private HazelcastInstance remoteInstance;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        localInstance = factory.newHazelcastInstance();
        remoteInstance = factory.newHazelcastInstance();
    }

    @Test
    public void destroyAllReplicasIncludingBackups() {
        IMap<Integer, Integer> map = localInstance.getMap(randomMapName());
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        map.destroy();

        assertAllPartitionContainersAreEmptyEventually(localInstance);
        assertAllPartitionContainersAreEmptyEventually(remoteInstance);
    }

    private void assertAllPartitionContainersAreEmptyEventually(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertAllPartitionContainersAreEmpty(instance);
            }
        });
    }

    private void assertAllPartitionContainersAreEmpty(HazelcastInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        int partitionCount = getPartitionCount(instance);

        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer container = context.getPartitionContainer(i);
            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            assertTrue(maps.isEmpty());
        }
    }

    private MapServiceContext getMapServiceContext(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }

    private int getPartitionCount(HazelcastInstance instance) {
        Node node = getNode(instance);
        return node.groupProperties.PARTITION_COUNT.getInteger();
    }
}
