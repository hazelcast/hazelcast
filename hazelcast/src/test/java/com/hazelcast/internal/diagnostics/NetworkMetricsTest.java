package com.hazelcast.internal.diagnostics;

import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;

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
