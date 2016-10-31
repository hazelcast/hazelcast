/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class DirtyBackupTest extends PartitionCorrectnessTestSupport {

    @Parameterized.Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {1, 2},
                {2, 3},
                {3, 4}
        });
    }

    @Test
    public void testPartitionData_withoutAntiEntropy() throws InterruptedException {
        startInstancesAndFillPartitions(false);
        assertSizeAndDataEventually(true);
    }

    @Test
    public void testPartitionData_withAntiEntropy() throws InterruptedException {
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
        FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
        cm.setDelayingPacketFilter(new BackupPacketReorderFilter(node.getSerializationService()));
    }

    private static class BackupPacketReorderFilter implements PacketFilter {
        final InternalSerializationService serializationService;

        BackupPacketReorderFilter(InternalSerializationService serializationService) {
            this.serializationService = serializationService;
        }

        @Override
        public boolean allow(Packet packet, Address endpoint) {
            return !packet.isFlagSet(Packet.FLAG_OP) || allowOperation(packet);
        }

        private boolean allowOperation(Packet packet) {
            try {
                ObjectDataInput input = serializationService.createObjectDataInput(packet);
                boolean identified = input.readBoolean();
                if (identified) {
                    int factory = input.readInt();
                    int type = input.readInt();
                    boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
                    return !isBackup;
                }
            } catch (IOException e) {
                throw new HazelcastException(e);
            }
            return true;
        }
    }
}
