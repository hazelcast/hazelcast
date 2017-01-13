package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeLevelTest extends HazelcastTestSupport {

    private ILogger logger;

    @Before
    public void setup() {
        logger = Logger.getLogger(MetricsRegistryImpl.class);
    }

    @Test
    public void test() {
        assertProbeExist(MANDATORY, MANDATORY);
        assertNotProbeExist(INFO, MANDATORY);
        assertNotProbeExist(DEBUG, MANDATORY);

        assertProbeExist(MANDATORY, INFO);
        assertProbeExist(INFO, INFO);
        assertNotProbeExist(DEBUG, INFO);

        assertProbeExist(MANDATORY, DEBUG);
        assertProbeExist(INFO, DEBUG);
        assertProbeExist(DEBUG, DEBUG);
    }

    public void assertProbeExist(ProbeLevel probeLevel, ProbeLevel minimumLevel) {
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(logger, minimumLevel);

        metricsRegistry.register(this, "foo", probeLevel, new LongProbeFunction<ProbeLevelTest>() {
            @Override
            public long get(ProbeLevelTest source) throws Exception {
                return 10;
            }
        });

        assertContains(metricsRegistry.getNames(), "foo");
        assertEquals(10, metricsRegistry.newLongGauge("foo").read());
    }

    public void assertNotProbeExist(ProbeLevel probeLevel, ProbeLevel minimumLevel) {
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(logger, minimumLevel);

        metricsRegistry.register(this, "foo", probeLevel, new LongProbeFunction<ProbeLevelTest>() {
            @Override
            public long get(ProbeLevelTest source) throws Exception {
                return 10;
            }
        });

        assertFalse(metricsRegistry.getNames().contains("foo"));
        assertEquals(0, metricsRegistry.newLongGauge("foo").read());
    }
}
