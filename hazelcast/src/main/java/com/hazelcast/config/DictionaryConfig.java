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

package com.hazelcast.config;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * This segment contains a single chunk of memory contolled by
 * {@link #setInitialSegmentSize(int)} and
 * {@link #setMaxSegmentSize(int)}. This chunk of memory contains the actual
 * map entries, but also hashtable(s) for lookup.
 *
 * In the default configuration a single partition uses 16384*256=4MB. But
 * segment memory allocation is done lazily.
 *
 * todo:
 * add option for eager memory allocation segment.
 */
public class DictionaryConfig {

    private static final int DEFAULT_INITIAL_SEGMENT_SIZE_BYTES = 16 * 1024;
    private static final int DEFAULT_MAX_SEGMENT_SIZE_BYTES = Integer.MAX_VALUE;
    private static final int DEFAULT_SEGMENTS_PER_PARTITION = 256;

    private Class keyClass;
    private Class valueClass;
    private String name;
    private int initialSegmentSize = DEFAULT_INITIAL_SEGMENT_SIZE_BYTES;
    private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE_BYTES;
    private int segmentsPerPartition = DEFAULT_SEGMENTS_PER_PARTITION;

    public DictionaryConfig(String name) {
        setName(name);
    }

    public DictionaryConfig(DictionaryConfig config) {
        this.name = config.name;
        this.keyClass = config.keyClass;
        this.valueClass = config.valueClass;
        this.initialSegmentSize = config.initialSegmentSize;
        this.maxSegmentSize = config.maxSegmentSize;
        this.segmentsPerPartition = config.segmentsPerPartition;
    }

    public Class getKeyClass() {
        return keyClass;
    }

    public DictionaryConfig setKeyClass(Class keyClass) {
        this.keyClass = checkNotNull(keyClass,"keyClass can't be null");
        return this;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public DictionaryConfig setValueClass(Class valueClass) {
        this.valueClass = checkNotNull(valueClass,"valueClass can't be null");
        return this;
    }

    public String getName() {
        return name;
    }

    public DictionaryConfig setName(String name) {
        this.name = checkHasText(name,"name can't be empty or null");
        return this;
    }

    public int getInitialSegmentSize() {
        return initialSegmentSize;
    }

    public DictionaryConfig setInitialSegmentSize(int initialSegmentSize) {
        this.initialSegmentSize = initialSegmentSize;
        return this;
    }

    public int getSegmentsPerPartition() {
        return segmentsPerPartition;
    }

    public DictionaryConfig setSegmentsPerPartition(int segmentsPerPartition) {
        this.segmentsPerPartition = segmentsPerPartition;
        return this;
    }

    public int getMaxSegmentSize() {
        return maxSegmentSize;
    }

    public void setMaxSegmentSize(int maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
    }

    public DictionaryConfig getAsReadOnly() {
        return this;
    }
}
