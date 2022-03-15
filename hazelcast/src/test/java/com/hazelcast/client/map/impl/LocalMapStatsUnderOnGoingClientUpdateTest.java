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

package com.hazelcast.client.map.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsUnderOnGoingClientUpdateTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance member = factory.newHazelcastInstance();
    private HazelcastInstance client;
    private String mapName = "test";

    @Before
    public void setUp() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        client = factory.newHazelcastClient(clientConfig);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void stats_generated_when_member_restarted_under_ongoing_client_update() throws Exception {
        IMap map = client.getMap(mapName);

        member.shutdown();

        member = factory.newHazelcastInstance();
        map.put(1, 1);
        map.put(2, 2);

        // get internal StatisticsAwareService.
        MapService mapService = getNodeEngineImpl(member).getService(SERVICE_NAME);
        Map<String, LocalMapStats> stats = ((StatisticsAwareService) mapService).getStats();
        LocalMapStats localMapStats = stats.get(mapName);

        // StatisticsAwareService should give right stats.
        assertNotNull("there should be 1 LocalMapStats object", localMapStats);
        assertEquals("Owned entry count should be 2", 2, localMapStats.getOwnedEntryCount());
    }
}
