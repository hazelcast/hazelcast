package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.internal.diagnostics.DiagnosticsPlugin.STATIC;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ConfigPropertiesPluginTest extends AbstractDiagnosticsPluginTest {

    private ConfigPropertiesPlugin plugin;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty("property1", "value1");
        HazelcastInstance hz = createHazelcastInstance(config);
        plugin = new ConfigPropertiesPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(STATIC, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() throws IOException {
        plugin.run(logWriter);
        assertContains("property1=value1");
    }
}
