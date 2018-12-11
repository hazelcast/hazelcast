/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.nio.tcp.OperationPacketFilter;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class AntiEntropyCorrectnessTest extends PartitionCorrectnessTestSupport {

    private static final float BACKUP_BLOCK_RATIO = 0.65f;

    @Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, 2},
                {1, InternalPartition.MAX_REPLICA_COUNT},
                {2, 3},
                {2, InternalPartition.MAX_REPLICA_COUNT},
                {3, 4},
                {3, InternalPartition.MAX_REPLICA_COUNT},
        });
    }

    @Test
    public void testPartitionData() {
        final HazelcastInstance[] instances = factory.newInstances(getConfig(true, true), nodeCount);
        for (HazelcastInstance instance : instances) {
            setBackupPacketDropFilter(instance, BACKUP_BLOCK_RATIO);
        }
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            fillData(instance);
        }

        assertSizeAndDataEventually();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    InternalPartitionServiceImpl partitionService = getNode(instance).partitionService;
                    int availablePermits = partitionService.getReplicaManager().availableReplicaSyncPermits();
                    assertEquals(PARALLEL_REPLICATIONS, availablePermits);
                }
            }
        }, 10);
    }

    public static void setBackupPacketDropFilter(HazelcastInstance instance, float blockRatio) {
        Node node = getNode(instance);
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
        cm.setPacketFilter(new BackupPacketDropFilter(node.getSerializationService(), blockRatio));
    }

    private static class BackupPacketDropFilter extends OperationPacketFilter implements PacketFilter {
        final float blockRatio;

        BackupPacketDropFilter(InternalSerializationService serializationService, float blockRatio) {
            super(serializationService);
            this.blockRatio = blockRatio;
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
            return !isBackup ? Action.ALLOW : (Math.random() > blockRatio ? Action.ALLOW : Action.DROP);
        }
    }
}
