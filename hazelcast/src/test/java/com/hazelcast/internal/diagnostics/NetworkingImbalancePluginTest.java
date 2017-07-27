package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NetworkingImbalancePluginTest extends AbstractDiagnosticsPluginTest {

    private NetworkingImbalancePlugin plugin;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "1");

        // we need to start a real Hazelcast instance here, since the mocked network doesn't have a TcpIpConnectionManager
        hz = Hazelcast.newHazelcastInstance(config);

        plugin = new NetworkingImbalancePlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @After
    public void tearDown() {
        hz.shutdown();
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
                hz.getMap("foo").put("key", "value");
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                plugin.run(logWriter);

                assertContains("Networking");
                assertContains("InputThreads");
                assertContains("OutputThreads");
            }
        });
    }
}
