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

package com.hazelcast.map.impl.querycache.event.sequence;

import com.hazelcast.internal.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * This class provides on-demand {@link PartitionSequencer} implementations
 * for subscriber side.
 *
 * @see PartitionSequencer
 */
public class DefaultSubscriberSequencerProvider implements SubscriberSequencerProvider {

    private static final ConstructorFunction<Integer, PartitionSequencer> PARTITION_SEQUENCER_CONSTRUCTOR
            = arg -> new DefaultPartitionSequencer();

    private final ConcurrentMap<Integer, PartitionSequencer> partitionSequences;

    public DefaultSubscriberSequencerProvider() {
        this.partitionSequences = new ConcurrentHashMap<>();
    }

    @Override
    public boolean compareAndSetSequence(long expect, long update, int partitionId) {
        PartitionSequencer sequence = getOrCreateSequence(partitionId);
        return sequence.compareAndSetSequence(expect, update);
    }

    @Override
    public long getSequence(int partitionId) {
        PartitionSequencer sequence = getOrCreateSequence(partitionId);
        return sequence.getSequence();
    }

    @Override
    public void reset(int partitionId) {
        PartitionSequencer sequence = getOrCreateSequence(partitionId);
        sequence.reset();
    }

    @Override
    public void resetAll() {
        for (PartitionSequencer partitionSequencer : partitionSequences.values()) {
            partitionSequencer.reset();
        }
    }

    private PartitionSequencer getOrCreateSequence(int partitionId) {
        return getOrPutIfAbsent(partitionSequences, partitionId, PARTITION_SEQUENCER_CONSTRUCTOR);
    }
}

