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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.FirewallingServer;
import com.hazelcast.internal.server.OperationPacketFilter;
import com.hazelcast.internal.server.PacketFilter;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DirtyBackupTest extends PartitionCorrectnessTestSupport {

    @Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1, 2},
                {2, 3},
                {3, 4},
        });
    }

    @Override
    protected Config getConfig() {
        // Partition count is overwritten back to PartitionCorrectnessTestSupport.partitionCount
        // in PartitionCorrectnessTestSupport.getConfig(boolean, boolean).
        return smallInstanceConfig();
    }

    @Test
    public void testPartitionData_withoutAntiEntropy() {
        startInstancesAndFillPartitions(false);
        assertSizeAndDataEventually(true);
    }

    @Test
    public void testPartitionData_withAntiEntropy() {
        startInstancesAndFillPartitions(true);
        assertSizeAndDataEventually(false);
    }

    private void startInstancesAndFillPartitions(boolean antiEntropyEnabled) {
        backupCount = 1;
        nodeCount = 3;

        HazelcastInstance[] instances = factory.newInstances(getConfig(true, antiEntropyEnabled), nodeCount);
        for (HazelcastInstance instance : instances) {
            setBackupPacketReorderFilter(instance);
        }
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            fillData(instance);
        }
    }

    private static void setBackupPacketReorderFilter(HazelcastInstance instance) {
        Node node = getNode(instance);
        FirewallingServer.FirewallingServerConnectionManager cm = (FirewallingServer.FirewallingServerConnectionManager)
                node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
        cm.setPacketFilter(new BackupPacketReorderFilter(node.getSerializationService()));
        cm.setDelayMillis(100, 1000);
    }

    private static class BackupPacketReorderFilter extends OperationPacketFilter implements PacketFilter {

        BackupPacketReorderFilter(InternalSerializationService serializationService) {
            super(serializationService);
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
            return isBackup ? Action.DELAY : Action.ALLOW;
        }
    }
}
