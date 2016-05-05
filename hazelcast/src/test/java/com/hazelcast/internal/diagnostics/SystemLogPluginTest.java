package com.hazelcast.internal.diagnostics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SystemLogPluginTest extends AbstractDiagnosticsPluginTest {

    private SystemLogPlugin plugin;
    private HazelcastInstance hz;
    private TestHazelcastInstanceFactory hzFactory;

    @Before
    public void setup() {
        setLoggingLog4j();
        hzFactory = createHazelcastInstanceFactory(2);
        hz = hzFactory.newHazelcastInstance();
        plugin = new SystemLogPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodSeconds(){
        assertEquals(1000, plugin.getPeriodMillis());
    }

    @Test
    public void testLifecycle() throws IOException {
        hz.shutdown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                logWriter.clean();
                plugin.run(logWriter);

                assertContains("Lifecycle[\n" +
                        "                          SHUTTING_DOWN]");
            }
        });
    }

    @Test
    public void testMember() throws IOException {
        hzFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                logWriter.clean();
                plugin.run(logWriter);
                assertContains("MemberAdded[");
            }
        });
    }
}
