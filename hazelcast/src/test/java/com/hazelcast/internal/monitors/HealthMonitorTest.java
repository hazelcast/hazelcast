package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.Metric;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.GroupProperties.PROP_HEALTH_MONITORING_DELAY_SECONDS;
import static com.hazelcast.instance.GroupProperties.PROP_HEALTH_MONITORING_LEVEL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HealthMonitorTest extends HazelcastTestSupport {
    private HealthMonitor healthMonitor;
    private HealthMonitor.HealthMetrics metrics;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(PROP_HEALTH_MONITORING_LEVEL, HealthMonitorLevel.NOISY.toString());
        config.setProperty(PROP_HEALTH_MONITORING_DELAY_SECONDS, "1");

        HazelcastInstance hz = createHazelcastInstance(config);
        healthMonitor = new HealthMonitor(getNode(hz));
        metricsRegistry = getMetricsRegistry(hz);
        metrics = healthMonitor.healthMetrics;
    }

    private void registerMetric(Metric metric, final int value) {
        metricsRegistry.register(this, metric.getName(), new DoubleProbeFunction<HealthMonitorTest>() {
            @Override
            public double get(HealthMonitorTest source) throws Exception {
                return value;
            }
        });
    }

    @Test
    public void exceedsThreshold_when_notTooHigh() {
        registerMetric(metrics.osProcessCpuLoad, 0);
        registerMetric(metrics.operationServicePendingInvocationsPercentage, 0);
        registerMetric(metrics.osSystemCpuLoad, 0);
        registerMetric(metrics.runtimeUsedMemory, 0);
        registerMetric(metrics.runtimeMaxMemory, 100);

        boolean result = metrics.exceedsThreshold();
        assertFalse(result);
    }

    @Test
    public void exceedsThreshold_when_osProcessCpuLoad_tooHigh() {
        registerMetric(metrics.osProcessCpuLoad, 90);
        boolean result = metrics.exceedsThreshold();
        assertTrue(result);
    }

    @Test
    public void exceedsThreshold_when_osSystemCpuLoad_TooHigh() {
        registerMetric(metrics.osSystemCpuLoad, 90);
        boolean result = metrics.exceedsThreshold();
        assertTrue(result);
    }

    @Test
    public void exceedsThreshold_operationServicePendingInvocationsPercentage() {
        registerMetric(metrics.operationServicePendingInvocationsPercentage, 90);
        boolean result = metrics.exceedsThreshold();
        assertTrue(result);
    }

    @Test
    public void exceedsThreshold_memoryUsedOfMaxPercentage() {
        registerMetric(metrics.runtimeUsedMemory, 90);
        registerMetric(metrics.runtimeMaxMemory, 100);
        metrics.update();
        boolean result = metrics.exceedsThreshold();
        assertTrue(result);
    }

    @Test
    public void render() {
        String s = metrics.render();

        assertContains(s,"processors=");

        //8, physical.memory.total= 9.8G, physical.memory.free=704.0M, swap.space.total=18.6G, swap.space.free=18.6G, heap.memory.used=26.3M, heap.memory.free=96.7M, heap.memory.total=123.0M, heap.memory.max=910.5M, heap.memory.used/total=0.00%, heap.memory.used/max=0.00%, minor.gc.count=0, minor.gc.time=0ms, major.gc.count=0, major.gc.time=0ms, os.processCpuLoad=0.33%, os.systemCpuLoad=1.00%, os.systemLoadAverage=34.00%, thread.count=31, thread.peakCount=31, cluster.timeDiff=9223372036854775807, event.q.size=0, executor.q.async.size=0, executor.q.client.size=0, executor.q.query.size=0, executor.q.scheduled.size=0, executor.q.io.size=0, executor.q.system.size=0, executor.q.mapLoad.size=0, executor.q.mapLoadAllKeys.size=0, executor.q.cluster.size=0, operations.completed.count=0, operations.executor.q.size=0, operations.executor.priority.q.size=0, operations.response.q.size=0, operations.running.count=0, operations.pending.invocations.percentage=0.00%, operations.pending.invocations.count=0, proxy.count=0, clientEndpoint.count=0, connection.active.count=0, client.connection.count=0, connection.count=0
        //System.out.println(s);
    }

    private void assertContains(String s, String expected){
        boolean contains = s.contains(expected);
        assertTrue(contains);
    }
}
