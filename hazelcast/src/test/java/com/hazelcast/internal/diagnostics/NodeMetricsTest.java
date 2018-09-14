package com.hazelcast.internal.diagnostics;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NodeMetricsTest extends AbstractMetricsTest {

    @Override
    Config configure() {
        return new Config().setProperty(Diagnostics.METRICS_LEVEL.getName(),
                ProbeLevel.INFO.name());
    }

    @Test
    public void clusterServiceStats() {
        assertHasStatsEventually(8, "cluster.");
        assertHasStatsEventually(6, "cluster.clock");
    }

    @Test
    public void proxyServiceStats() {
        assertHasStatsEventually(3, "proxy.");
    }

    @Test
    public void memoryStats() {
        assertHasStatsEventually(12, "memory.");
    }

    @Test
    public void gcStats() {
        assertHasStatsEventually(6, "gc.");
    }

    @Test
    public void runtimeStats() {
        assertHasStatsEventually(6, "runtime.");
    }

    @Test
    public void classloadingStats() {
        assertHasStatsEventually(3, "classloading.");
    }

    @Test
    public void osStats() {
        assertHasStatsEventually(11, "os.");
    }

    @Test
    public void threadStats() {
        assertHasStatsEventually(4, "thread.");
    }

    @Test
    public void userHomeStats() {
        assertHasStatsEventually(4, "file.partition", "user.home");
    }

    @Test
    public void partitionServiceStats() {
        assertHasStatsEventually(10, "partitions.");
    }

    @Test
    public void transactionServiceStats() {
        assertHasStatsEventually(3, "transactions.");
    }

    @Test
    public void clientServiceStats() {
        assertHasStatsEventually(2, "client.endpoint.");
    }

    @Test
    public void operationsServiceStats() {
        assertHasStatsEventually(32, "operation.");
        assertHasStatsEventually(5, "operation.responses.");
        assertHasStatsEventually(3, "operation.invocations.");
        assertHasStatsEventually(2, "operation.parker.");
    }
}
