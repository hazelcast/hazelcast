package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PerformanceLogLoggerTest extends HazelcastTestSupport {
    private PerformanceLogLogger performanceLog;
    private MockLogger logger;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(PerformanceMonitor.ENABLED.getName(), "true");
        config.setProperty(PerformanceMonitor.SKIP_FILE.getName(), "true");
        config.setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");
        HazelcastProperties properties = new HazelcastProperties(config);

        logger = new MockLogger();
        HazelcastThreadGroup threadGroup = new HazelcastThreadGroup("foo", logger, getClass().getClassLoader());
        PerformanceMonitor performanceMonitor = new PerformanceMonitor("dontcare", logger, threadGroup, properties);
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(logger, ProbeLevel.INFO);

        performanceMonitor.register(new MetricsPlugin(logger, metricsRegistry, properties));
        performanceMonitor.register(new SystemPropertiesPlugin(logger));

        performanceLog = (PerformanceLogLogger) performanceMonitor.performanceLog;
    }

    @Test
    public void test() throws InterruptedException {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String content = logger.sb.toString();

                System.out.println(content);

                assertNotNull(content);

                assertTrue(content.contains("SystemProperties["));
                assertTrue(content.contains("Metrics["));
            }
        });
    }

    private class MockLogger extends AbstractLogger {
        private StringBuffer sb = new StringBuffer();

        @Override
        public void log(Level level, String message) {
            sb.append(message).append("\n");
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log(LogEvent logEvent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Level getLevel() {
            return Level.ALL;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }
    }

}
