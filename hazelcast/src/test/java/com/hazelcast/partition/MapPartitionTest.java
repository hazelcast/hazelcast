package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.partition.domain.Customer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPartitionTest extends HazelcastTestSupport {

    @Test
    public void nonDefaultPartitionCount_MapInitilizationTests() throws Exception {

        final int nodeCount=4;
        final int partitionCount = 2711;
        final int totalKeys=40;
        final String mapName = randomString();

        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
        //TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        final MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);

        config.addMapConfig(mapConfig);

        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        for (int i = 0; i < nodeCount; i++) {
            instances[i] = Hazelcast.newHazelcastInstance(config);
            //factory.newHazelcastInstance(config);
        }

        for (int i = 0; i < nodeCount; i++) {
            assertClusterSizeEventually(nodeCount, instances[i], 10);
        }

        for (int i = 0; i < nodeCount; i++) {
            warmUpPartitions(instances[i]);
        }

        Thread[] threads = new Thread[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            final int idx=i;

            threads[i] = new Thread(new Runnable() {
                public void run() {
                    HazelcastInstance h = instances[idx];

                    final IMap map = h.getMap(mapName);
                    final Member localMember = h.getCluster().getLocalMember();
                    final PartitionService partitionService = h.getPartitionService();
                    for(int i=0; i<totalKeys; i++){
                        Partition partition = partitionService.getPartition(i);
                        if (localMember.equals(partition.getOwner())) {
                            map.put(i, new Customer());
                        }
                    }
                }
            });
            threads[i].start();
        }
        assertJoinable(threads);

        final IMap map = instances[0].getMap(mapName);
        assertEquals(map.size(), totalKeys);
    }

}
