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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.util.ConstructorFunction;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;

/**
 * Responsible for partition-sequence and partition UUID generation.
 * Used by invalidator to generate metadata for invalidation events.
 * <p>
 * This metadata is used by {@link RepairingHandler} and {@link RepairingTask}
 * to act against possible invalidation-miss and partition-loss.
 * <p>
 * One instance per service is created. Used on member side.
 */
public class MetaDataGenerator {

    private final int partitionCount;
    private final ConstructorFunction<String, AtomicLongArray> sequenceGeneratorConstructor
            = new ConstructorFunction<String, AtomicLongArray>() {
        @Override
        public AtomicLongArray createNew(String arg) {
            return new AtomicLongArray(partitionCount);
        }
    };
    private final ConcurrentMap<Integer, UUID> uuids = new ConcurrentHashMap<Integer, UUID>();
    private final ConcurrentMap<String, AtomicLongArray> sequenceGenerators = new ConcurrentHashMap<String, AtomicLongArray>();
    private final ConstructorFunction<Integer, UUID> uuidConstructor
            = new ConstructorFunction<Integer, UUID>() {
        @Override
        public UUID createNew(Integer partitionId) {
            return newUnsecureUUID();
        }
    };

    public MetaDataGenerator(int partitionCount) {
        assert partitionCount > 0;

        this.partitionCount = partitionCount;
    }

    public long currentSequence(String name, int partitionId) {
        AtomicLongArray sequences = sequenceGenerators.get(name);
        if (sequences == null) {
            return 0;
        }
        return sequences.get(partitionId);
    }

    public long nextSequence(String name, int partitionId) {
        return sequenceGenerator(name).incrementAndGet(partitionId);
    }

    public void setCurrentSequence(String name, int partitionId, long sequence) {
        sequenceGenerator(name).set(partitionId, sequence);
    }

    private AtomicLongArray sequenceGenerator(String name) {
        return getOrPutIfAbsent(sequenceGenerators, name, sequenceGeneratorConstructor);
    }

    public UUID getOrCreateUuid(int partitionId) {
        return getOrPutIfAbsent(uuids, partitionId, uuidConstructor);
    }

    public UUID getUuidOrNull(int partitionId) {
        return uuids.get(partitionId);
    }

    public void setUuid(int partitionId, UUID uuid) {
        uuids.put(partitionId, uuid);
    }

    public void removeUuidAndSequence(final int partitionId) {
        // remove UUID
        uuids.remove(partitionId);

        // reset data-structures' sequence numbers
        for (AtomicLongArray sequences : sequenceGenerators.values()) {
            sequences.set(partitionId, 0);
        }
    }

    public void destroyMetaDataFor(String dataStructureName) {
        sequenceGenerators.remove(dataStructureName);
    }

    public void regenerateUuid(int partitionId) {
        uuids.put(partitionId, uuidConstructor.createNew(partitionId));
    }

    public void resetSequence(String name, int partitionId) {
        sequenceGenerator(name).set(partitionId, 0);
    }

    // used for testing
    public ConcurrentMap<String, AtomicLongArray> getSequenceGenerators() {
        return sequenceGenerators;
    }
}
