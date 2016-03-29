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
import org.junit.Test;

import java.io.IOException;

public abstract class AbstractAntiEntropyCorrectnessTest extends PartitionCorrectnessTestSupport {

    private static final float BACKUP_BLOCK_RATIO = 0.65f;

    @Test
    public void testPartitionData() throws InterruptedException {
        HazelcastInstance[] instances = factory.newInstances(getConfig(backupCount, true, true), nodeCount);
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
            cm.setPacketFilter(new BackupPacketFilter(node.getSerializationService(), BACKUP_BLOCK_RATIO));
        }
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            fillData(instance);
        }

        assertSizeAndData();
    }

    private static class BackupPacketFilter implements PacketFilter {
        final InternalSerializationService serializationService;
        final float blockRatio;

        BackupPacketFilter(InternalSerializationService serializationService, float blockRatio) {
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
                boolean identified = input.readBoolean();
                if (identified) {
                    int factory = input.readInt();
                    int type = input.readInt();
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
