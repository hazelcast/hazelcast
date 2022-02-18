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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapServiceTest extends HazelcastTestSupport {

    private NodeEngine nodeEngine;

    @Before
    public void setUp() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        nodeEngine = getNodeEngineImpl(hazelcastInstance);
    }

    @Test
    public void testShutdown() {
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);
        service.init(nodeEngine, null);

        service.shutdown(true);
    }

    @Test
    public void testShutdown_withoutInit() {
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);

        service.shutdown(true);
    }

    @Test
    public void testGetLocalReplicatedMapStatsNoObjectGenerationIfDisabledStats() {
        String name = randomMapName();
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName(name);
        replicatedMapConfig.setStatisticsEnabled(false);
        nodeEngine.getConfig().addReplicatedMapConfig(replicatedMapConfig);
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);

        LocalReplicatedMapStats stats = service.getLocalReplicatedMapStats(name);
        LocalReplicatedMapStats stats2 = service.getLocalReplicatedMapStats(name);
        LocalReplicatedMapStats stats3 = service.getLocalReplicatedMapStats(name);
        assertSame(stats, stats2);
        assertSame(stats2, stats3);
    }
}
