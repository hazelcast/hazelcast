package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;
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
import java.util.Map;
import java.util.Random;

import static com.hazelcast.test.TestStringUtils.fileAsText;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class StoreLatencyPlugin_MapIntegrationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private Map<String, String> map;

    @Before
    public void setup() throws Exception {
        setLoggingLog4j();

        Config config = new Config()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.storeLatency.period.seconds", "1");

        MapConfig mapConfig = addMapConfig(config);

        hz = createHazelcastInstance(config);
        map = hz.getMap(mapConfig.getName());
    }

    @After
    public void after(){
        File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
        file.delete();
    }

    @Test
    public void test() throws Exception {
        for (int k = 0; k < 100; k++) {
            map.get(k);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
                String content = fileAsText(file);
                assertTrue(content.contains("mappy"));
            }
        });
    }

    private static MapConfig addMapConfig(Config config) {
        MapConfig mapConfig = config.getMapConfig("mappy");
        mapConfig.getMapStoreConfig().setEnabled(true).setImplementation(new MapStore() {
            private final Random random = new Random();

            @Override
            public void store(Object key, Object value) {

            }

            @Override
            public Object load(Object key) {
                randomSleep();
                return null;
            }

            private void randomSleep() {
                long delay = random.nextInt(100);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public Map loadAll(Collection keys) {
                return null;
            }

            @Override
            public void storeAll(Map map) {

            }

            @Override
            public void delete(Object key) {

            }

            @Override
            public Iterable loadAllKeys() {
                return null;
            }

            @Override
            public void deleteAll(Collection keys) {

            }
        });
        return mapConfig;
    }
}

