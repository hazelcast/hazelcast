package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.RingbufferStore;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;

import static com.hazelcast.test.TestStringUtils.fileAsText;
import static org.junit.Assert.assertTrue;

public class StoreLatencyPlugin_RingbufferIntegrationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private Ringbuffer<Object> rb;

    @Before
    public void setup() throws Exception {
        Config config = new Config()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.storeLatency.period.seconds", "1");

        RingbufferConfig rbConfig = addRingbufferConfig(config);

        hz = createHazelcastInstance(config);
        rb = hz.getRingbuffer(rbConfig.getName());
    }

    @After
    public void after(){
        File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
        file.delete();
    }

    @Test
    public void test() throws Exception {
        for (long k = 0; k <= rb.tailSequence(); k++) {
            rb.readOne(k);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
                String content = fileAsText(file);
                assertTrue(content.contains("ringworm"));
            }
        });
    }

    private static RingbufferConfig addRingbufferConfig(Config config) {
        RingbufferStore store = new RingbufferStore() {
            private final Random random = new Random();

            @Override
            public void store(long sequence, Object data) {

            }

            @Override
            public void storeAll(long firstItemSequence, Object[] items) {

            }

            @Override
            public Object load(long sequence) {
                randomSleep();
                return sequence;
            }

            @Override
            public long getLargestSequence() {
                randomSleep();
                return 100;
            }

            private void randomSleep() {
                long delay = 1 + random.nextInt(100);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        return config.getRingbufferConfig("ringworm")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setStoreImplementation(store));
    }
}
