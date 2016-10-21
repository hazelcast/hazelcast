package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.QueueStore;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.test.TestStringUtils.fileAsText;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class StoreLatencyPlugin_QueueIntegrationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private IQueue queue;

    @Before
    public void setup() throws Exception {
        Config config = new Config()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.storeLatency.period.seconds", "1");

        QueueConfig queueConfig = addQueueConfig(config);

        hz = createHazelcastInstance(config);
        queue = hz.getQueue(queueConfig.getName());
    }

    @After
    public void after(){
        File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
        file.delete();
    }


    @Test
    public void test() throws Exception {
        for (int k = 0; k < 100; k++) {
            queue.put(k);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
                String content = fileAsText(file);
                assertTrue(content.contains("somequeue"));
            }
        });
    }

    private static QueueConfig addQueueConfig(Config config) {
        QueueConfig queueConfig = config.getQueueConfig("somequeue");
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueConfig.setQueueStoreConfig(queueStoreConfig);
        queueStoreConfig.setEnabled(true).setStoreImplementation(new QueueStore() {
            private final Random random = new Random();

            @Override
            public void store(Long key, Object value) {
                randomSleep();
            }

            @Override
            public void delete(Long key) {
                randomSleep();
            }

            @Override
            public void storeAll(Map map) {
                randomSleep();
            }

            @Override
            public void deleteAll(Collection keys) {
                randomSleep();
            }

            @Override
            public Map loadAll(Collection keys) {
                randomSleep();
                return new HashMap();
            }

            @Override
            public Set<Long> loadAllKeys() {
                return new HashSet<Long>();
            }

            @Override
            public Object load(Long key) {
                randomSleep();
                return key;
            }

            private void randomSleep() {
                long delay = random.nextInt(100);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        return queueConfig;
    }
}

