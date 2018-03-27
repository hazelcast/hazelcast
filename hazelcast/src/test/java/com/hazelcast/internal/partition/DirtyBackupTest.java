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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.nio.tcp.OperationPacketFilter;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class DirtyBackupTest extends PartitionCorrectnessTestSupport {

    @Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1, 2},
                {2, 3},
                {3, 4},
        });
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
        FirewallingConnectionManager cm = (FirewallingConnectionManager) node.getConnectionManager();
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
