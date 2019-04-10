/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.util.function.Supplier;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for the {@link com.hazelcast.datastream.DataStream}.
 */
public class DataStreamConfig {
    private static final int KB = 1024;

    private String name;
    private Class valueClass;
    // todo: is there a good reason to make initial not equal to max?
    private long initialSegmentSize = KB * KB;
    private int maxSegmentSize = KB * KB;
    private int segmentsPerPartition = Integer.MAX_VALUE;
    private Set<String> indices = new HashSet<String>();
    private boolean storageEnabled = false;
    private File storageDir = new File("datastream-storage");
    // to get rid of 'too' old segments.
    // so data is inserted on the segment as long as the tenuringAgeMillis has not been reached.
    // once it is reached, a new segment is created and this one is moved one to the back.
    // perhaps call it tenuring age?

    // if you want to do time based insertion, perhaps best to keep the maxSegmentSize as Long.MAX_VALUE?
    private long tenuringAgeMillis = Long.MAX_VALUE;

    private Map<String, Supplier<Aggregator>> attachedAggregators = new HashMap<String, Supplier<Aggregator>>();



    public DataStreamConfig() {
    }

    public DataStreamConfig(String name) {
        setName(name);
    }

    public DataStreamConfig(DataStreamConfig defConfig) {
        this.name = defConfig.name;
        this.valueClass = defConfig.valueClass;
        this.initialSegmentSize = defConfig.initialSegmentSize;
        this.maxSegmentSize = defConfig.maxSegmentSize;
        this.segmentsPerPartition = defConfig.segmentsPerPartition;
        this.tenuringAgeMillis = defConfig.tenuringAgeMillis;
        this.indices = defConfig.indices;
        this.storageEnabled = defConfig.storageEnabled;
        this.storageDir = defConfig.storageDir;
    }

    public File getStorageDir() {
        return storageDir;
    }

    public DataStreamConfig setStorageDir(File storageDir) {
        this.storageDir = checkNotNull(storageDir,"storageDir can't be null");
        return this;
    }

    public boolean isStorageEnabled() {
        return storageEnabled;
    }

    public void setStorageEnabled(boolean storageEnabled) {
        this.storageEnabled = storageEnabled;
    }

    public Map<String, Supplier<Aggregator>> getAttachedAggregators() {
        return attachedAggregators;
    }

    public DataStreamConfig addAttachedAggregator(String id, Supplier<Aggregator> supplier) {
        checkNotNull(id, "id can't be null");
        checkNotNull(supplier,"supplier can't be null");

        this.attachedAggregators.put(id, supplier);
        return this;
    }

    public DataStreamConfig addIndexField(String field) {
        checkNotNull(field, "field can't be null");

        indices.add(field);
        return this;
    }

    public Set<String> getIndices() {
        return indices;
    }

    public void setIndices(Set<String> indices) {
        this.indices = indices;
    }

    public long getTenuringAgeMillis() {
        return tenuringAgeMillis;
    }

    public DataStreamConfig setTenuringAgeMillis(long tenuringAgeMillis) {
        this.tenuringAgeMillis = tenuringAgeMillis;
        return this;
    }

    public DataStreamConfig setTenuringAge(int age, TimeUnit unit) {
        return setTenuringAgeMillis(unit.toMillis(age));
    }

//    public int getMaxSegmentCount() {
//        return maxSegmentCount;
//    }
//
//    public DataSeriesConfig setMaxSegmentCount(int maxSegmentCount) {
//        this.maxSegmentCount = maxSegmentCount;
//        return this;
//    }

    public int getMaxSegmentSize() {
        return maxSegmentSize;
    }

    /**
     * Sets the maximum segment size in bytes.
     *
     * @param maxSegmentSize
     * @return
     */
    public DataStreamConfig setMaxSegmentSize(int maxSegmentSize) {
        this.maxSegmentSize = checkPositive(maxSegmentSize, "maxSegmentSize should be larger than 0");
        return this;
    }

    public int getSegmentsPerPartition() {
        return segmentsPerPartition;
    }

    /**
     * Sets the maximum number of segments per partition.
     *
     * @param segmentsPerPartition
     * @return
     */
    public DataStreamConfig setSegmentsPerPartition(int segmentsPerPartition) {
        this.segmentsPerPartition = checkPositive(segmentsPerPartition, "segmentsPerPartition should be larger than 0");
        return this;
    }

    public long getInitialSegmentSize() {
        return initialSegmentSize;
    }

    /**
     * Sets the size in bytes for the initial segment size.
     *
     * @param initialSegmentSize
     * @return
     */
    public DataStreamConfig setInitialSegmentSize(long initialSegmentSize) {
        this.initialSegmentSize = checkPositive(initialSegmentSize, "initialSegmentSize should be larger than 0");
        return this;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public DataStreamConfig setValueClass(Class valueClass) {
        this.valueClass = checkNotNull(valueClass, "valueClass");
        return this;
    }

    public void setName(String name) {
        this.name = checkHasText(name,"name is empty or null");
    }

    public String getName() {
        return name;
    }

    public DataStreamConfig getAsReadOnly() {
        return this;
    }

    @Override
    public String toString() {
        return "DataStreamConfig{"
                + "name='" + name + '\''
                + ", valueClass=" + valueClass
                + ", initialSegmentSize=" + initialSegmentSize
                + ", maxSegmentSize=" + maxSegmentSize
                + ", segmentsPerPartition=" + segmentsPerPartition
                + ", indices=" + indices
                + ", tenuringAgeMillis=" + tenuringAgeMillis
                + ", attachedAggregators=" + attachedAggregators
                + '}';
    }
}
