package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlRemovingMappingTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(3, null, new ClientConfig());
    }

    @Test
    public void test_strange() throws InterruptedException {
        HazelcastInstance client = client();

        String mapName = randomName();
        IMap<Long, UUID> map = client.getMap(mapName);
        for (int i = 0; i < 100_000; i++) {
            map.put((long) i, UUID.randomUUID());
        }
        createMapping(mapName, Long.class, UUID.class);
        logger.info("Map created");

        List<HazelcastInstance> instanceList = new ArrayList<>();
        for (int i = 0; i < instances().length; i++) {
            HazelcastInstance instance = instances()[i];
            instanceList.add(instance);
            instance.getLifecycleService().addLifecycleListener(event -> {
                logger.info("New state: " + event.getState());
            });
        }

        for (int i = 0; i < 5; i++) {
            HazelcastInstance instance = factory().newHazelcastInstance(smallInstanceConfig());
            instance.getLifecycleService().addLifecycleListener(event -> {
                logger.info("New state: " + event.getState());
            });
            assertClusterSizeEventually(4 + i, client);
            waitAllForSafeState(instanceList);
            client.getSql().execute("select * from " + mapName);
        }
    }
}
