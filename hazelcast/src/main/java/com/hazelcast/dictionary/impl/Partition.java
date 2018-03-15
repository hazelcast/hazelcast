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

package com.hazelcast.dictionary.impl;

import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.dataseries.MemoryInfo;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.util.HashUtil.hashToIndex;

public class Partition {

    private final NodeEngine nodeEngine;
    private final SerializationService serializationService;
    private final DictionaryConfig config;
    private Segment[] segments;

    public Partition(NodeEngine nodeEngine,
                     DictionaryConfig config,
                     EntryModel model,
                     EntryEncoder encoder) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
        this.config = config;
        this.segments = new Segment[config.getSegmentsPerPartition()];
        for (int k = 0; k < segments.length; k++) {
            segments[k] = new Segment(nodeEngine.getSerializationService(), model, encoder, config);
        }
    }

    public Segment[] segments() {
        return segments;
    }

    public Segment segment(int hash) {
        return segments[hashToIndex(hash, segments.length)];
    }

    public MemoryInfo memoryInfo() {
        long consumedBytes = 0;
        long allocatedBytes = 0;
        int segmentsInUse = 0;
        long count = 0;

        for (Segment segment : segments) {
            if (segment.isAllocated()) {
                segmentsInUse++;
            }
            count += segment.count();
            allocatedBytes += segment.allocated();
        }

        return new MemoryInfo(-1, allocatedBytes, segmentsInUse, count);
    }
}
