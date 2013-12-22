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

import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.LifecycleMapper;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;

public class MapCombineTask<KeyIn, ValueIn, KeyOut, ValueOut, Chunk> implements Runnable {

    private final Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    private final CombinerFactory<KeyOut, ValueOut, Chunk> combinerFactory;

    private final MappingPhase<KeyOut, ValueOut> mappingPhase;

    private final int chunkSize;

    public MapCombineTask(int chunkSize, MappingPhase<KeyOut, ValueOut> mappingPhase,
                          Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                          CombinerFactory<KeyOut, ValueOut, Chunk> combinerFactory) {
        this.mapper = mapper;
        this.chunkSize = chunkSize;
        this.mappingPhase = mappingPhase;
        this.combinerFactory = combinerFactory;
    }

    @Override
    public final void run() {
        DefaultContext<KeyOut, ValueOut> context = createContext();

        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).initialize(context);
        }
        mappingPhase.executeMappingPhase(context);
        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).finalized(context);
        }

        Map<KeyOut, Object> chunkMap = context.finish();
        // TODO Send final chunk from this partition
        // Wrap into LastChunkResponse object
    }

    private DefaultContext<KeyOut, ValueOut> createContext() {
        return new DefaultContext<KeyOut, ValueOut>(combinerFactory, this);
    }

    void onEmit(DefaultContext<KeyOut, ValueOut> context) {
        if (context.getCollected() == chunkSize) {
            Map<KeyOut, Chunk> chunkMap = context.requestChunk();
            // TODO Send chunkMap to reducers
            // Wrap into IntermediateChunkResponse object
        }
    }

}
