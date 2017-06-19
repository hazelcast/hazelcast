package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class AntiEntropyCleanupTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "nodeCount:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1},
                {2}
        });
    }

    @Parameterized.Parameter
    public int nodeCount;

    @Test
    public void testCleanup() {
        Config config = new Config().setProperty(PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance[] instances = factory.newInstances(config, nodeCount);
        warmUpPartitions(instances);

        String mapName = randomMapName();

        HazelcastInstance instance1 = instances[0];
        for (int partitionId = 0; partitionId < getPartitionService(instance1).getPartitionCount(); partitionId++) {
            String key = generateKeyForPartition(instance1, partitionId);
            instance1.getMap(mapName).put(key, key);
        }

        instance1.getMap(mapName).destroy();

        for (final HazelcastInstance instance : instances) {
            final InternalPartitionService partitionService = getPartitionService(instance);
            final PartitionReplicaVersionManager replicaVersionManager = partitionService.getPartitionReplicaVersionManager();

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                        for (ServiceNamespace namespace : replicaVersionManager.getNamespaces(partitionId)) {
                            assertFalse(namespace.getServiceName().equals(MapService.SERVICE_NAME));
                        }
                    }
                }
            });
        }
    }

}
