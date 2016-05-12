package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * This test can't run with test-hazelcast instances since we rely on a real TcpIpConnectionManager.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OverloadedConnectionsPluginTest extends AbstractDiagnosticsPluginTest {

    private OverloadedConnectionsPlugin plugin;
    private HazelcastInstance local;
    private HazelcastInstance remote;
    private volatile boolean stop;
    private String remoteKey;
    private InternalSerializationService serializationService;

    @Before
    public void setup() throws InterruptedException {
        Hazelcast.shutdownAll();

        Config config = new Config();
        config.setProperty(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName(), "1");
        config.setProperty(OverloadedConnectionsPlugin.SAMPLES.getName(), "10");
        config.setProperty(OverloadedConnectionsPlugin.THRESHOLD.getName(), "10");

        local = Hazelcast.newHazelcastInstance(config);
        serializationService = getSerializationService(local);
        remote = Hazelcast.newHazelcastInstance(config);

        plugin = new OverloadedConnectionsPlugin(getNodeEngineImpl(local));
        plugin.onStart();

        warmUpPartitions(local, remote);
        remoteKey = generateKeyOwnedBy(remote);
    }

    @After
    public void tearDown() {
        stop = true;
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        spawn(new Runnable() {
            @Override
            public void run() {
                IMap map = local.getMap("foo");
                while (!stop) {
                    map.getAsync(remoteKey);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                plugin.run(logWriter);

                assertContains(GetOperation.class.getSimpleName() + " sampleCount=");
            }
        });
    }

    @Test
    public void toKey() {
        assertToKey(DummyOperation.class.getName(), new DummyOperation());
        assertToKey(Integer.class.getName(), new Integer(10));
        assertToKey(Backup.class.getName() + "#" + DummyOperation.class.getName(),
                new Backup(new DummyOperation(), getAddress(local), new long[0], true));
    }

    private void assertToKey(String key, Object object) {
        Packet packet = new Packet(serializationService.toBytes(object));
        assertEquals(key, plugin.toKey(packet));
    }
}
