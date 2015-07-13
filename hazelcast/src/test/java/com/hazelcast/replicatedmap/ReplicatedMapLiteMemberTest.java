package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapLiteMemberTest extends HazelcastTestSupport {

    @Test
    public void testLiteMembersWithReplicatedMap() throws Exception {
        final Config config = buildConfig(false);
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance lite = nodeFactory.newHazelcastInstance(buildConfig(true));

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");

        map1.put("key", "value");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(instance2.getReplicatedMap("default").containsKey("key"));
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final ReplicatedMapService service = getReplicatedMapService(lite);
                assertEquals(0, service.getAllReplicatedRecordStores("default").size());
            }
        }, 5);
    }


    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedMapOnLiteMember() {
        final HazelcastInstance lite = createSingleLiteMember();
        lite.getReplicatedMap("default");
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedStoreOnLiteMember() {
        final HazelcastInstance lite = createSingleLiteMember();
        final ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", true, 1);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testGetReplicatedStoreOnLiteMember() {
        final HazelcastInstance lite = createSingleLiteMember();
        final ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", false, 1);
    }


    private HazelcastInstance createSingleLiteMember() {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        return nodeFactory.newHazelcastInstance(buildConfig(true));
    }

    private ReplicatedMapService getReplicatedMapService(HazelcastInstance lite) {
        final NodeEngineImpl nodeEngine = getNodeEngineImpl(lite);
        return nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
    }

    private Config buildConfig(final boolean liteMember) {
        final Config config = new Config();
        config.setLiteMember(liteMember);
        return config;
    }

}
