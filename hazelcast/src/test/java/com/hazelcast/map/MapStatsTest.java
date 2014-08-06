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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapStatsTest extends HazelcastTestSupport {

    @Test
    public void testLocalMultiMapStats() throws InterruptedException {
        Config config = new Config();
        final String mapName = randomString();
        final String item0 = randomString();
        final String item1 = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        MultiMap<Object, Object> multiMap0 = instances[0].getMultiMap(mapName);
        MultiMap<Object, Object> multiMap1 = instances[1].getMultiMap(mapName);

        // InternalPartitionService is used in order to determine owners of these keys that we will use.
        InternalPartitionService partitionService = getNode(instances[0]).getPartitionService();
        Address address = partitionService.getPartitionOwner(partitionService.getPartitionId(item1));

        boolean isItem1inFirstInstance = address.equals(getNode(instances[0]).getThisAddress());
        multiMap0.get(item0);

        LocalMultiMapStats firstInstanceMapStats = multiMap0.getLocalMultiMapStats();
        LocalMultiMapStats secondInstanceMapStats = multiMap1.getLocalMultiMapStats();
        assertEquals(0, firstInstanceMapStats.getHits());
        assertEquals(0, secondInstanceMapStats.getHits());
        assertEquals(1, firstInstanceMapStats.getGetOperationCount());
        assertEquals(0, secondInstanceMapStats.getGetOperationCount());

        multiMap0.put(item1, 1);
        assertEquals(1, multiMap0.getLocalMultiMapStats().getPutOperationCount());
        assertEquals(0, multiMap1.getLocalMultiMapStats().getPutOperationCount());

        multiMap1.get(item1);
        firstInstanceMapStats = multiMap0.getLocalMultiMapStats();
        secondInstanceMapStats = multiMap1.getLocalMultiMapStats();

        assertEquals(1, firstInstanceMapStats.getGetOperationCount());
        assertEquals(isItem1inFirstInstance ? 1 : 0, firstInstanceMapStats.getHits());

        assertEquals(1, secondInstanceMapStats.getGetOperationCount());
        assertEquals(isItem1inFirstInstance ? 0 : 1, secondInstanceMapStats.getHits());

        multiMap0.get(item1);
        multiMap1.get(item1);
        LocalMultiMapStats localMultiMapStats1 = multiMap1.getLocalMultiMapStats();
        assertEquals(2, localMultiMapStats1.getGetOperationCount());
        assertEquals(isItem1inFirstInstance ? 0 : 3, localMultiMapStats1.getHits());
    }
}
