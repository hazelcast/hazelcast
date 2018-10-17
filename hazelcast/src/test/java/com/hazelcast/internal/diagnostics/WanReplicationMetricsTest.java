/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.DefaultNodeContext;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.monitor.impl.WanSyncStateImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.WanSyncStatus;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

/**
 * These tests are isolated as they require special setup including real network IO.
 */
@RunWith(HazelcastParallelClassRunner.class)
public class WanReplicationMetricsTest extends AbstractMetricsIntegrationTest {

    private HazelcastInstance hz;

    @Before
    public void setUp() {
        Config config = new Config().setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name());
        hz = HazelcastInstanceFactory.newHazelcastInstance(config, randomName(),
                new WanServiceMockingNodeContext());
        setCollectionContext(getNode(hz).nodeEngine.getMetricsRegistry().openContext(ProbeLevel.INFO));
    }

    @After
    public void cleanup() {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void wanReplicationPublisherStats() {
        assertEventuallyHasStatsWith(6, "ns=wan instance=config target=publisher");
    }

    @Test
    public void wanReplicationSyncStatus() {
        assertEventuallyHasAllStats(
                "ns=wan-sync instance=config2 target=publisher2 syncedPartitionCount",
                "ns=wan-sync instance=config2 target=publisher2 status",
                "ns=wan-sync instance=config2 target=publisher2 creationTime");
    }

    private static class FakeWanReplicationService extends WanReplicationServiceImpl {

        private final Map<String, LocalWanStats> stats;
        private final WanSyncState state;

        public FakeWanReplicationService(Node node) {
            super(node);
            LocalWanStatsImpl stats = new LocalWanStatsImpl();
            LocalWanPublisherStatsImpl pubStats = new LocalWanPublisherStatsImpl();
            pubStats.setConnected(true);
            pubStats.setState(WanPublisherState.REPLICATING);
            pubStats.setOutboundQueueSize(20);
            stats.setLocalPublisherStatsMap(Collections
                    .<String, LocalWanPublisherStats>singletonMap("publisher", pubStats));
            this.stats = Collections.<String, LocalWanStats>singletonMap("config", stats);
            this.state = new WanSyncStateImpl(WanSyncStatus.IN_PROGRESS, 42, "config2", "publisher2");
        }

        @Override
        public WanSyncState getWanSyncState() {
            return state;
        }

        @Override
        public Map<String, LocalWanStats> getStats() {
            return stats;
        }
    }

    private static class WanServiceMockingNodeContext extends DefaultNodeContext {
        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new WanServiceMockingNodeExtension(node, new FakeWanReplicationService(node));
        }
    }
    private static class WanServiceMockingNodeExtension extends DefaultNodeExtension {
        private final WanReplicationService wanReplicationService;
        WanServiceMockingNodeExtension(Node node, WanReplicationService wanReplicationService) {
            super(node);
            this.wanReplicationService = wanReplicationService;
        }
        @SuppressWarnings("unchecked")
        @Override
        public <T> T createService(Class<T> clazz) {
            return clazz.isAssignableFrom(WanReplicationService.class)
                    ? (T) wanReplicationService : super.createService(clazz);
        }
    }
}
