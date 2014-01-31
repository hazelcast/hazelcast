/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class DefaultContext<KeyIn, ValueIn>
        implements Context<KeyIn, ValueIn> {

    private final Map<KeyIn, Combiner<KeyIn, ValueIn, ?>> combiners = new HashMap<KeyIn, Combiner<KeyIn, ValueIn, ?>>();
    private final CombinerFactory<KeyIn, ValueIn, ?> combinerFactory;
    private final MapCombineTask mapCombineTask;

    private volatile int partitionId;
    private volatile int collected = 0;

    protected DefaultContext(CombinerFactory<KeyIn, ValueIn, ?> combinerFactory,
                             MapCombineTask mapCombineTask) {
        this.mapCombineTask = mapCombineTask;
        this.combinerFactory = combinerFactory != null ?
                combinerFactory : new CollectingCombinerFactory<KeyIn, ValueIn>();
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void emit(KeyIn key, ValueIn value) {
        Combiner<KeyIn, ValueIn, ?> combiner = getOrCreateCombiner(key);
        combiner.combine(key, value);
        collected++;
        mapCombineTask.onEmit(this, partitionId);
    }

    public <Chunk> Map<KeyIn, Chunk> requestChunk() {
        Map<KeyIn, Chunk> chunkMap = new HashMap<KeyIn, Chunk>(combiners.size());
        for (Map.Entry<KeyIn, Combiner<KeyIn, ValueIn, ?>> entry : combiners.entrySet()) {
            Chunk chunk = (Chunk) entry.getValue().finalizeChunk();
            chunkMap.put(entry.getKey(), chunk);
        }
        collected = 0;
        return chunkMap;
    }

    public int getCollected() {
        return collected;
    }

    public <Chunk> Map<KeyIn, Chunk> finish() {
        for (Combiner<KeyIn, ValueIn, ?> combiner : combiners.values()) {
            combiner.finalizeCombine();
        }
        return requestChunk();
    }

    private Combiner<KeyIn, ValueIn, ?> getOrCreateCombiner(KeyIn key) {
        Combiner<KeyIn, ValueIn, ?> combiner = combiners.get(key);
        if (combiner == null) {
            combiner = combinerFactory.newCombiner(key);
            combiners.put(key, combiner);
            combiner.beginCombine();
        }
        return combiner;
    }

    private static class CollectingCombinerFactory<KeyIn, ValueIn>
            implements CombinerFactory<KeyIn, ValueIn, List<ValueIn>> {

        @Override
        public Combiner<KeyIn, ValueIn, List<ValueIn>> newCombiner(KeyIn key) {
            return new Combiner<KeyIn, ValueIn, List<ValueIn>>() {

                private final List<ValueIn> values = new CopyOnWriteArrayList<ValueIn>();

                @Override
                public void combine(KeyIn key, ValueIn value) {
                    values.add(value);
                }

                @Override
                public List<ValueIn> finalizeChunk() {
                    List<ValueIn> values = new ArrayList<ValueIn>(this.values);
                    this.values.clear();
                    return values;
                }
            };
        }
    }

}
