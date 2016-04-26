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

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InvocationPluginTest extends AbstractPerformanceMonitorPluginTest {

    private InvocationPlugin plugin;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(InvocationPlugin.SAMPLE_PERIOD_SECONDS.getName(), "1");
        config.setProperty(InvocationPlugin.SLOW_THRESHOLD_SECONDS.getName(), "5");

        hz = createHazelcastInstance(config);

        plugin = new InvocationPlugin(getNodeEngineImpl(hz));
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
                hz.getMap("foo").executeOnKey(randomString(), new SlowEntryProcessor());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                logWriter.write(plugin);
                assertContains(EntryOperation.class.getName());
            }
        });
    }

    static class SlowEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException ignored) {
            }
            return null;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }
    }
}
