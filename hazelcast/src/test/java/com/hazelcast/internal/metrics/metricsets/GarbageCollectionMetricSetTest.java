package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.Metric;
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

    private MetricsRegistryImpl blackbox;
    private GarbageCollectionMetricSet.GcStats gcStats;

    @Before
    public void setup() {
        blackbox = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
        gcStats = new GarbageCollectionMetricSet.GcStats();
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(GarbageCollectionMetricSet.class);
    }

    @Test
    public void minorCount() {
        final Gauge gauge = blackbox.getGauge("gc.minorCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                 assertEquals(gcStats.minorCount, gauge.readLong(), 1);
            }
        });
    }

    @Test
    public void minorTime() throws InterruptedException {
        final Gauge gauge = blackbox.getGauge("gc.minorTime");
         assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.minorTime, gauge.readLong(), SECONDS.toMillis(1));
            }
        });
    }

    @Test
    public void majorCount() {
        final Gauge gauge = blackbox.getGauge("gc.majorCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.majorCount, gauge.readLong(), 1);
            }
        });
    }

    @Test
    public void majorTime() {
        final Gauge gauge = blackbox.getGauge("gc.majorTime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.majorTime, gauge.readLong(), SECONDS.toMillis(1));
            }
        });
    }


    @Test
    public void unknownCount() {
        final Gauge gauge = blackbox.getGauge("gc.unknownCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.unknownCount, gauge.readLong(), 1);
            }
        });
    }

    @Test
    public void unknownTime() {
        final Gauge gauge = blackbox.getGauge("gc.unknownTime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.unknownTime, gauge.readLong(), SECONDS.toMillis(1));
            }
        });
    }
}
