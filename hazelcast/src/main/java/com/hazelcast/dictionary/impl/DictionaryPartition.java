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
import com.hazelcast.dictionary.AggregationRecipe;
import com.hazelcast.dictionary.MemoryInfo;
import com.hazelcast.dictionary.impl.type.EntryType;
import com.hazelcast.spi.NodeEngine;

import static com.hazelcast.util.HashUtil.hashToIndex;

/**
 * Contains all the data for a single partition of the
 * {@link com.hazelcast.dictionary.Dictionary}.
 *
 * A DictionaryPartition is build up out of a fixed number
 * of segments. Segments are 'threadsafe' self contained units
 * this contain the hashtable for the key, the dataregion for
 * the key/value storage.
 */
public class DictionaryPartition {

    private Segment[] segments;

    public DictionaryPartition(NodeEngine nodeEngine,
                               DictionaryConfig config,
                               EntryType entryType,
                               EntryEncoder encoder) {
        this.segments = new Segment[config.getSegmentsPerPartition()];
        for (int k = 0; k < segments.length; k++) {
            segments[k] = new Segment(nodeEngine.getSerializationService(), entryType, encoder, config);
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
            consumedBytes += segment.consumed();
            count += segment.count();
            allocatedBytes += segment.allocated();
        }

        return new MemoryInfo(consumedBytes, allocatedBytes, segmentsInUse, count);
    }

    public void prepareAggregation(String preparationId, AggregationRecipe recipe) {
        //todo
    }
}
