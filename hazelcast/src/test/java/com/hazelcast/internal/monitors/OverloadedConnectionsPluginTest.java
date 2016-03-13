package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_OVERLOADED_CONNECTIONS_PERIOD_SECONDS;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_OVERLOADED_CONNECTIONS_SAMPLES;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_OVERLOADED_CONNECTIONS_THRESHOLD;
import static org.junit.Assert.assertEquals;

/**
 * This test can't run with test-hazelcast instances since we rely on a real TcpIpConnectionManager.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OverloadedConnectionsPluginTest extends AbstractPerformanceMonitorPluginTest {

    private OverloadedConnectionsPlugin plugin;
    private HazelcastInstance local;
    private HazelcastInstance remote;
    private volatile boolean stop;
    private String remoteKey;
    private SerializationService serializationService;

    @Before
    public void setup() throws InterruptedException {
        Hazelcast.shutdownAll();

        Config config = new Config();
        config.setProperty(PERFORMANCE_MONITOR_OVERLOADED_CONNECTIONS_PERIOD_SECONDS, "1");
        config.setProperty(PERFORMANCE_MONITOR_OVERLOADED_CONNECTIONS_SAMPLES, "10");
        config.setProperty(PERFORMANCE_MONITOR_OVERLOADED_CONNECTIONS_THRESHOLD, "10");

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
                logWriter.write(plugin);
                assertContains(GetOperation.class.getSimpleName() + " sampleCount=");
            }
        });
    }

    @Test
    public void toKey(){
        assertToKey(DummyOperation.class.getName(), new DummyOperation());
        assertToKey(Integer.class.getName(), new Integer(10));
        assertToKey(Backup.class.getName()+"#"+DummyOperation.class.getName(),
                new Backup(new DummyOperation(), getAddress(local),new long[0],true));
    }

    private void assertToKey(String key, Object object){
        Packet packet = new Packet(serializationService.toBytes(object));
        assertEquals(key, plugin.toKey(packet));
    }
}
