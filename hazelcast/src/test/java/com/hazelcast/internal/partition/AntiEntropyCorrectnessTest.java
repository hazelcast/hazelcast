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
public class AntiEntropyCorrectnessTest extends PartitionCorrectnessTestSupport {

    private static final float BACKUP_BLOCK_RATIO = 0.65f;

    @Parameterized.Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, 2},
                {1, InternalPartition.MAX_REPLICA_COUNT},
                {2, 3},
                {2, InternalPartition.MAX_REPLICA_COUNT},
                {3, 4},
                {3, InternalPartition.MAX_REPLICA_COUNT}
        });
    }

    @Test
    public void testPartitionData() throws InterruptedException {
        HazelcastInstance[] instances = factory.newInstances(getConfig(true, true), nodeCount);
        for (HazelcastInstance instance : instances) {
            setBackupPacketDropFilter(instance, BACKUP_BLOCK_RATIO);
        }
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            fillData(instance);
        }

        assertSizeAndDataEventually();
    }

    public static void setBackupPacketDropFilter(HazelcastInstance instance, float blockRatio) {
        Node node = getNode(instance);
        FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
        cm.setPacketFilter(new BackupPacketDropFilter(node.getSerializationService(), blockRatio));
    }

    private static class BackupPacketDropFilter implements PacketFilter {
        final InternalSerializationService serializationService;
        final float blockRatio;

        BackupPacketDropFilter(InternalSerializationService serializationService, float blockRatio) {
            this.serializationService = serializationService;
            this.blockRatio = blockRatio;
        }

        @Override
        public boolean allow(Packet packet, Address endpoint) {
            return !packet.isFlagSet(Packet.FLAG_OP) || allowOperation(packet);
        }

        private boolean allowOperation(Packet packet) {
            try {
                ObjectDataInput input = serializationService.createObjectDataInput(packet);
                byte header = input.readByte();
                boolean identified = (header & 1 << 0) != 0;
                if (identified) {
                    boolean compressed = (header & 1 << 2) != 0;
                    int factory = compressed ? input.readByte() : input.readInt();
                    int type = compressed ? input.readByte() : input.readInt();
                    boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
                    return !isBackup || Math.random() > blockRatio;
                }
            } catch (IOException e) {
                throw new HazelcastException(e);
            }
            return true;
        }
    }
}
