/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL;
import static com.hazelcast.test.Accessors.getPartitionService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AntiEntropyCleanupTest extends HazelcastTestSupport {

    @Parameters(name = "nodeCount:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1},
                {2},
        });
    }

    @Parameter
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
