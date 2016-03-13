package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.operation.EntryOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_SLOW_OPERATIONS_PERIOD_SECONDS;
import static com.hazelcast.instance.GroupProperty.SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS;
import static com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_ENABLED;
import static com.hazelcast.instance.GroupProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SlowOperationPluginTest extends AbstractPerformanceMonitorPluginTest {

    private SlowOperationPlugin plugin;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(SLOW_OPERATION_DETECTOR_ENABLED, "true");
        config.setProperty(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "1000");
        config.setProperty(SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS, "1000");
        config.setProperty(PERFORMANCE_MONITOR_SLOW_OPERATIONS_PERIOD_SECONDS, "1");

        hz = createHazelcastInstance(config);

        plugin = new SlowOperationPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(1000, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() {
        spawn(new Runnable() {
            @Override
            public void run() {
                hz.getMap("foo").executeOnKey("bar", new SlowEntryProcessor());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                logWriter.write(plugin);
                assertContains(EntryOperation.class.getName());
                assertContains("stackTrace");
                assertContains("invocations=1");
                assertContains("startedAt=");
                assertContains("durationNs=");
                assertContains("operationDetails=");
            }
        });
    }

    static class SlowEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }
    }
}
