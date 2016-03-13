package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.operation.EntryOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_PENDING_INVOCATIONS_PERIOD_SECONDS;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_PENDING_INVOCATIONS_THRESHOLD;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PendingInvocationsPluginTest extends AbstractPerformanceMonitorPluginTest {

    private HazelcastInstance hz;
    private PendingInvocationsPlugin plugin;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(PERFORMANCE_MONITOR_PENDING_INVOCATIONS_PERIOD_SECONDS, "1");
        config.setProperty(PERFORMANCE_MONITOR_PENDING_INVOCATIONS_THRESHOLD, "1");

        hz = createHazelcastInstance(config);

        plugin = new PendingInvocationsPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() throws IOException {
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

                assertContains("PendingInvocations[");
                assertContains("count=1");
                assertContains(EntryOperation.class.getName() + "=1");
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
