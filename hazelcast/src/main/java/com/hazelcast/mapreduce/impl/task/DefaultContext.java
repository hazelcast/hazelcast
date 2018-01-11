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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.core.IFunction;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.impl.CombinerResultList;
import com.hazelcast.mapreduce.impl.MapReduceUtil;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.IConcurrentMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.util.ConcurrentReferenceHashMap.ReferenceType.STRONG;
import static com.hazelcast.util.MapUtil.createHashMapAdapter;

/**
 * This is the internal default implementation of a map reduce context mappers emit values to. It controls the emitted
 * values to be combined using either the set {@link com.hazelcast.mapreduce.Combiner} or by utilizing the internal
 * collecting combiner (which is just a better HashMap ;-)).<br/>
 * In addition to that it is responsible to notify about an the {@link com.hazelcast.mapreduce.impl.task.MapCombineTask}
 * about an emitted value to eventually send out chunks on reaching the chunk size limit.
 *
 * @param <KeyIn>
 * @param <ValueIn>
 */
public class DefaultContext<KeyIn, ValueIn>
        implements Context<KeyIn, ValueIn> {

    private static final AtomicIntegerFieldUpdater<DefaultContext> COLLECTED = AtomicIntegerFieldUpdater
            .newUpdater(DefaultContext.class, "collected");
    private final IConcurrentMap<KeyIn, Combiner<ValueIn, ?>> combiners =
            new ConcurrentReferenceHashMap<KeyIn, Combiner<ValueIn, ?>>(STRONG, STRONG);

    private final CombinerFactory<KeyIn, ValueIn, ?> combinerFactory;
    private final MapCombineTask mapCombineTask;

    private final IFunction<KeyIn, Combiner<ValueIn, ?>> combinerFunction = new CombinerFunction();

    // This field is only accessed through the updater
    private volatile int collected;

    private volatile int partitionId;

    private volatile InternalSerializationService serializationService;

    protected DefaultContext(CombinerFactory<KeyIn, ValueIn, ?> combinerFactory, MapCombineTask mapCombineTask) {
        this.mapCombineTask = mapCombineTask;
        this.combinerFactory = combinerFactory != null ? combinerFactory : new CollectingCombinerFactory<KeyIn, ValueIn>();
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void emit(KeyIn key, ValueIn value) {
        Combiner<ValueIn, ?> combiner = getOrCreateCombiner(key);
        combiner.combine(value);
        COLLECTED.incrementAndGet(this);
        mapCombineTask.onEmit(this, partitionId);
    }

    public <Chunk> Map<KeyIn, Chunk> requestChunk() {
        int mapSize = MapReduceUtil.mapSize(combiners.size());
        Map<KeyIn, Chunk> chunkMap = createHashMapAdapter(mapSize);
        for (Map.Entry<KeyIn, Combiner<ValueIn, ?>> entry : combiners.entrySet()) {
            Combiner<ValueIn, ?> combiner = entry.getValue();
            Chunk chunk = (Chunk) combiner.finalizeChunk();
            combiner.reset();

            if (chunk != null) {
                chunkMap.put(entry.getKey(), chunk);
            }
        }
        COLLECTED.set(this, 0);
        return chunkMap;
    }

    public int getCollected() {
        return collected;
    }

    public void finalizeCombiners() {
        for (Combiner<ValueIn, ?> combiner : combiners.values()) {
            combiner.finalizeCombine();
        }
    }

    public Combiner<ValueIn, ?> getOrCreateCombiner(KeyIn key) {
        return combiners.applyIfAbsent(key, combinerFunction);
    }

    public void setSerializationService(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    /**
     * This {@link com.hazelcast.mapreduce.CombinerFactory} implementation is used
     * if no specific CombinerFactory was set in the configuration of the job to
     * do mapper aside combining of the emitted values.<br/>
     *
     * @param <KeyIn>   type of the key
     * @param <ValueIn> type of the value
     */
    @BinaryInterface
    private static class CollectingCombinerFactory<KeyIn, ValueIn>
            implements CombinerFactory<KeyIn, ValueIn, List<ValueIn>> {

        @Override
        public Combiner<ValueIn, List<ValueIn>> newCombiner(KeyIn key) {
            return new Combiner<ValueIn, List<ValueIn>>() {

                private final List<ValueIn> values = new ArrayList<ValueIn>();

                @Override
                public void combine(ValueIn value) {
                    values.add(value);
                }

                @Override
                public List<ValueIn> finalizeChunk() {
                    return new CombinerResultList<ValueIn>(this.values);
                }

                @Override
                public void reset() {
                    this.values.clear();
                }
            };
        }
    }

    @SerializableByConvention
    private class CombinerFunction implements IFunction<KeyIn, Combiner<ValueIn, ?>> {
        @Override
        public Combiner<ValueIn, ?> apply(KeyIn keyIn) {
            Combiner<ValueIn, ?> combiner = combinerFactory.newCombiner(keyIn);
            combiner.beginCombine();
            return combiner;
        }
    }
}
