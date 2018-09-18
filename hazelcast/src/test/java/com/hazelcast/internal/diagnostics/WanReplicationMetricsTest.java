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
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;
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
public class WanReplicationMetricsTest extends AbstractMetricsTest {

    private HazelcastInstance hz;
    private ProbeRenderContext renderContext;

    @Before
    public void setUp() {
        Config config = new Config().setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name());
        hz = HazelcastInstanceFactory.newHazelcastInstance(config, randomName(),
                new WanServiceMockingNodeContext());
        renderContext = getNode(hz).nodeEngine.getProbeRegistry().newRenderingContext();
    }

    @After
    public void cleanup() {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Override
    protected ProbeRenderContext getRenderContext() {
        return renderContext;
    }

    @Test
    public void wanReplicationPublisherStats() {
        assertHasStatsEventually(6, "type=wan instance=config target=publisher");
    }

    @Test
    public void wanReplicationSyncStatus() {
        assertHasAllStatsEventually(
                "type=wan instance=config2 target=publisher2 sync.syncedPartitionCount",
                "type=wan instance=config2 target=publisher2 sync.status");
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
