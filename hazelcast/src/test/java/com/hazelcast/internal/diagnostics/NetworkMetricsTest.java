package com.hazelcast.internal.diagnostics;

import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;

import java.util.Collections;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.config.WanPublisherState;
import com.hazelcast.instance.DefaultNodeContext;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.monitor.impl.WanSyncStateImpl;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.WanSyncStatus;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

@RunWith(HazelcastParallelClassRunner.class)
public class NetworkMetricsTest extends DefaultMetricsTest {

    /**
     * In order to get the TCP statistics we need a real {@link ConnectionManager}.
     */
    @Rule
    public final OverridePropertyRule useRealNetwork = set(HAZELCAST_TEST_USE_NETWORK, "true");

    @Test
    public void connectionManagerStats() {
        assertHasStatsEventually(10, "tcp.connection.");
    }

    @Test
    public void healthMonitorMetrics() {
        assertHasAllStatsEventually(
                "tcp.connection.activeCount",
                "tcp.connection.count",
                "tcp.connection.clientCount");
    }

}
