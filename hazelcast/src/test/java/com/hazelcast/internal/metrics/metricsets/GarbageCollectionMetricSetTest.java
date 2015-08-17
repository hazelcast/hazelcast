package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class GarbageCollectionMetricSetTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;
    private GarbageCollectionMetricSet.GcProbes gcProbes;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
        gcProbes = new GarbageCollectionMetricSet.GcProbes();
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(GarbageCollectionMetricSet.class);
    }

    @Test
    public void minorCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.minorCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcProbes.update();
                assertEquals(gcProbes.minorCount, gauge.read(), 1);
            }
        });
    }

    @Test
    public void minorTime() throws InterruptedException {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.minorTime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcProbes.update();
                assertEquals(gcProbes.minorTime, gauge.read(), SECONDS.toMillis(1));
            }
        });
    }

    @Test
    public void majorCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.majorCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcProbes.update();
                assertEquals(gcProbes.majorCount, gauge.read(), 1);
            }
        });
    }

    @Test
    public void majorTime() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.majorTime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcProbes.update();
                assertEquals(gcProbes.majorTime, gauge.read(), SECONDS.toMillis(1));
            }
        });
    }


    @Test
    public void unknownCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.unknownCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcProbes.update();
                assertEquals(gcProbes.unknownCount, gauge.read(), 1);
            }
        });
    }

    @Test
    public void unknownTime() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.unknownTime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcProbes.update();
                assertEquals(gcProbes.unknownTime, gauge.read(), SECONDS.toMillis(1));
            }
        });
    }
}
