package com.hazelcast.jet.sql.impl.misc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientSqlResubmissionMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlResubmissionTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(3, null, new ClientConfig().setSqlResubmissionMode(ClientSqlResubmissionMode.RETRY_ALL));
    }

    @Test
    public void test_strange() {
        HazelcastInstance client = client();

        String mapName = randomName();
        IMap<Long, UUID> map = client.getMap(mapName);
        for (int i = 0; i < 100_000; i++) {
            map.put((long) i, UUID.randomUUID());
        }
        createMapping(mapName, Long.class, UUID.class);
        logger.info("Map created");

        for (int i = 0; i < 5; i++) {
            factory().newHazelcastInstance(smallInstanceConfig());
            assertClusterSizeEventually(4 + i, client);
            client.getSql().execute("select * from " + mapName);
        }
    }

    @Test
    public void test_simple() {
        HazelcastInstance instance = instances()[1];
        HazelcastInstance client = client();

        assertClusterSizeEventually(3, instance);

        String mapName = randomName();
        IMap<Long, UUID> map = client.getMap(mapName);
        for (int i = 0; i < 1_000_000; i++) {
            map.put((long) i, UUID.randomUUID());
        }

        logger.info("Map created");

        SqlResult rows = client.getSql().execute("select * from table(generate_stream(1))");
        new Thread(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
            instance.getLifecycleService().terminate();
        }).start();

        try {
            for (SqlRow row : rows) {
                System.out.println("Jest");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_stress() {
        HazelcastInstance instance = instances()[1];
        HazelcastInstance client = client();

        assertClusterSizeEventually(3, instance);

        String mapName = randomName();
        IMap<Long, UUID> map = client.getMap(mapName);
        for (int i = 0; i < 100_000; i++) {
            map.put((long) i, UUID.randomUUID());
        }
        createMapping(mapName, Long.class, UUID.class);
        logger.info("Map created");

        new Thread(() -> {
            while (!Thread.interrupted()) {
                try {
                    logger.info("Creating new instance");
                    HazelcastInstance tempInstance = factory().newHazelcastInstance(smallInstanceConfig());
                    assertClusterSizeEventually(4, tempInstance);
                    Thread.sleep(2000);
                    logger.info("Terminating instance");
//                    tempInstance.getLifecycleService().terminate();
                } catch (Exception e) {
                }
            }
        }).start();

        try {
            while (!Thread.interrupted()) {
                int count = 0;
                SqlResult rows = client.getSql().execute("select * from " + mapName);
                for (SqlRow row : rows) {
                    count++;
                }
                System.out.println(count);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
