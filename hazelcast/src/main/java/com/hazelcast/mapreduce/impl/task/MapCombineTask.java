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
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.LifecycleMapper;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.nio.Address;

import java.util.Map;

import static com.hazelcast.mapreduce.impl.MapReduceUtil.mapResultToMember;

public class MapCombineTask<KeyIn, ValueIn, KeyOut, ValueOut, Chunk> implements Runnable {

    private final Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;
    private final CombinerFactory<KeyOut, ValueOut, Chunk> combinerFactory;
    private final MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> mappingPhase;
    private final KeyValueSource<KeyIn, ValueIn> keyValueSource;
    private final MapReduceService mapReduceService;
    private final String name;
    private final String jobId;
    private final int chunkSize;

    public MapCombineTask(JobTaskConfiguration configuration, MapReduceService mapReduceService,
                          MappingPhase<KeyIn, ValueIn, KeyOut, ValueOut> mappingPhase) {

        this.mappingPhase = mappingPhase;
        this.mapReduceService = mapReduceService;
        this.mapper = configuration.getMapper();
        this.name = configuration.getName();
        this.jobId = configuration.getJobId();
        this.chunkSize = configuration.getChunkSize();
        this.combinerFactory = configuration.getCombinerFactory();
        this.keyValueSource = configuration.getKeyValueSource();
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public final void run() {
        DefaultContext<KeyOut, ValueOut> context = createContext();

        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).initialize(context);
        }
        mappingPhase.executeMappingPhase(keyValueSource, context);
        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).finalized(context);
        }

        Map<KeyOut, Chunk> chunkMap = context.finish();
        // Wrap into LastChunkNotification object
        Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(mapReduceService, chunkMap);
        for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
            mapReduceService.sendNotification(entry.getKey(),
                    new LastChunkNotification(entry.getValue(), name, jobId));
        }
    }

    private DefaultContext<KeyOut, ValueOut> createContext() {
        return new DefaultContext<KeyOut, ValueOut>(combinerFactory, this);
    }

    void onEmit(DefaultContext<KeyOut, ValueOut> context) {
        if (context.getCollected() == chunkSize) {
            Map<KeyOut, Chunk> chunkMap = context.requestChunk();
            // Wrap into IntermediateChunkNotification object
            Map<Address, Map<KeyOut, Chunk>> mapping = mapResultToMember(mapReduceService, chunkMap);
            for (Map.Entry<Address, Map<KeyOut, Chunk>> entry : mapping.entrySet()) {
                mapReduceService.sendNotification(entry.getKey(),
                        new IntermediateChunkNotification(entry.getValue(), name, jobId));
            }
        }
    }

}
