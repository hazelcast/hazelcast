package com.hazelcast.internal.monitors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BuildInfoPluginTest extends AbstractPerformanceMonitorPluginTest {

    private BuildInfoPlugin plugin;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new BuildInfoPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(PerformanceMonitorPlugin.STATIC, plugin.getPeriodMillis());
    }

    @Test
    public void test() throws IOException {
        plugin.run(logWriter);

        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

        assertContains("BuildNumber=" + buildInfo.getBuildNumber());
        assertContains("Build=" + buildInfo.getBuild());
        assertContains("Revision=" + buildInfo.getRevision());
        assertContains("Version=" + buildInfo.getVersion());
        assertContains("SerialVersion=" + buildInfo.getSerializationVersion());
        assertContains("Enterprise=" + buildInfo.isEnterprise());
    }
}
