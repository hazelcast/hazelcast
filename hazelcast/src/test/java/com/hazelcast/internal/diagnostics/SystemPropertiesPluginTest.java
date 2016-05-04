package com.hazelcast.internal.diagnostics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SystemPropertiesPluginTest extends AbstractDiagnosticsPluginTest {

    public static final String FAKE_PROPERTY = "hazelcast.fake.property";
    public static final String FAKE_PROPERTY_VALUE = "foobar";

    private SystemPropertiesPlugin plugin;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new SystemPropertiesPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
        System.setProperty(FAKE_PROPERTY, "foobar");
    }

    @After
    public void tearDown() {
        System.clearProperty(FAKE_PROPERTY);
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(DiagnosticsPlugin.STATIC, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() throws IOException {
        plugin.run(logWriter);

        Properties systemProperties = System.getProperties();

        // we check a few of the regular ones.
        assertContains("java.class.version=" + systemProperties.get("java.class.version"));
        assertContains("java.class.path=" + systemProperties.get("java.class.path"));

        // we want to make sure the hazelcast system properties are added
        assertContains(FAKE_PROPERTY + "=" + FAKE_PROPERTY_VALUE);

        // we don't want to have awt
        assertNotContains("java.awt");
    }
}
