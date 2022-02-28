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

package com.hazelcast.ringbuffer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.ringbuffer.impl.RingbufferService.getRingbufferNamespace;
import static com.hazelcast.test.Accessors.getFirstBackupInstance;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionIdViaReflection;
import static java.util.Collections.emptyList;

final class RingbufferTestUtil {

    private RingbufferTestUtil() {
    }

    /**
     * Returns all backup items of a {@link Ringbuffer} by a given ringbuffer instance.
     * <p>
     * Note: Returns the backups from the first replica index.
     *
     * @param instances  the {@link HazelcastInstance} array to gather the data from
     * @param ringbuffer the {@link Ringbuffer} to retrieve the backups from
     * @return a {@link Collection} with the backup items
     */
    static Collection<Object> getBackupRingbuffer(HazelcastInstance[] instances, Ringbuffer ringbuffer) {
        int partitionId = getPartitionIdViaReflection(ringbuffer);
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        return getBackupRingbuffer(backupInstance, partitionId, ringbuffer.getName());
    }

    /**
     * Returns all backup items of a {@link Ringbuffer} by a given ringbuffer name.
     * <p>
     * Note: You have to provide the {@link HazelcastInstance} you want to retrieve the backups from.
     * Use {@link getBackupInstance} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HazelcastInstance} to retrieve the backups from
     * @param partitionId    the partition ID of the ringbuffer
     * @param ringbufferName the ringbuffer name
     * @return a {@link Collection} with the backup items
     */
    static Collection<Object> getBackupRingbuffer(HazelcastInstance backupInstance, int partitionId, String ringbufferName) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(backupInstance);
        RingbufferService service = nodeEngine.getService(RingbufferService.SERVICE_NAME);
        RingbufferContainer container = service.getContainerOrNull(partitionId, getRingbufferNamespace(ringbufferName));
        if (container == null) {
            return emptyList();
        }

        SerializationService serializationService = nodeEngine.getSerializationService();
        List<Object> backupRingbuffer = new ArrayList<Object>((int) container.size());
        for (long sequence = container.headSequence(); sequence <= container.tailSequence(); sequence++) {
            backupRingbuffer.add(serializationService.toObject(container.readAsData(sequence)));
        }
        return backupRingbuffer;
    }
}
