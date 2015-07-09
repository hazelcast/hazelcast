package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.mapstore.writebehind.MapStoreWithCounter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindBackupPartitionCleanerTest extends HazelcastTestSupport {

    @Test
    public void testAdd() throws Exception {
        String mapName = randomMapName();
        MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        Config config = getConfig(mapName, mapStore);
        HazelcastInstance node = createHazelcastInstance(config);

        IMap map = node.getMap(mapName);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        MapStoreContext mapStoreContext = getMapStoreContext(map, mapName);
        WriteBehindBackupPartitionCleaner cleaner = new WriteBehindBackupPartitionCleaner(mapStoreContext);
        cleaner.add(0);

        cleaner.removeFromBackups();

    }


    private MapStoreContext getMapStoreContext(Map map, String mapName) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService service = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        return mapServiceContext.getMapContainer(mapName).getMapStoreContext();
    }

    @Test
    public void testRemoveFromBackups() throws Exception {

    }

    private Config getConfig(String mapName, MapStoreWithCounter mapStore) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setImplementation(mapStore)
                .setWriteDelaySeconds(100)
                .setWriteCoalescing(false);

        Config config = new Config();
        config.getMapConfig(mapName)
                .setBackupCount(1)
                .setMapStoreConfig(mapStoreConfig);

        return config;
    }
}