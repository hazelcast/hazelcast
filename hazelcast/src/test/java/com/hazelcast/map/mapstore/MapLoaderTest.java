package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapLoaderTest extends HazelcastTestSupport {

    //https://github.com/hazelcast/hazelcast/issues/1770
    @Test
    public void test1770() throws InterruptedException {
        Config config = new Config();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl("http://127.0.0.1:8090/mancenter");

        MapConfig mapConfig = new MapConfig("foo");

        final AtomicBoolean loadAllCalled = new AtomicBoolean();
        MapLoader mapLoader = new MapLoader() {
            @Override
            public Object load(Object key) {
                return null;
            }

            @Override
            public Map loadAll(Collection keys) {
                loadAllCalled.set(true);
                return new HashMap();
            }

            @Override
            public Set loadAllKeys() {
                return new HashSet(Arrays.asList(1));
            }
        };
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(mapLoader);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        config.addMapConfig(mapConfig);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        Map map = hz.getMap(mapConfig.getName());

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse("LoadAll should not have been called", loadAllCalled.get());
            }
        }, 10);
    }
}
