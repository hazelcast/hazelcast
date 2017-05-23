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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.PASSIVE;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ExpirationAgainstClusterStateTest extends HazelcastTestSupport {

    private static final int TTL_SECONDS = 5;
    private static final String MAP_NAME = "test";

    private Cluster cluster;
    private IMap<Integer, Integer> map;

    @Before
    public void setUp() throws Exception {
        Config config = new Config();

        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setTimeToLiveSeconds(TTL_SECONDS);

        HazelcastInstance node = createHazelcastInstance(config);
        map = node.getMap(MAP_NAME);

        cluster = node.getCluster();
    }

    @Test
    public void expired_entries_removed_after_cluster_state_changes() throws Exception {
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        cluster.changeClusterState(PASSIVE);

        sleepSeconds(TTL_SECONDS * 3);

        cluster.changeClusterState(ACTIVE);

        assertSizeEventually(0, map);
    }
}
