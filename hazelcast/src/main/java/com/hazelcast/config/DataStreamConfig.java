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
import com.hazelcast.memory.MemoryUnit;
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
    private int initialRegionSize = -1;
    private int maxRegionSize = KB * KB;
    private int maxRegionsPerPartition = Integer.MAX_VALUE;
    private Set<String> indices = new HashSet<String>();
    private boolean storageEnabled;
    private File storageDir = new File("datastream-storage");
    // to get rid of 'too' old regions.
    // so data is inserted on the regions as long as the tenuringAgeMillis has not been reached.
    // once it is reached, a new region is created and this one is moved one to the back.
    // perhaps call it tenuring age?

    // if you want to do time based insertion, perhaps best to keep the maxRegionSize as Long.MAX_VALUE?
    private long tenuringAgeMillis = Long.MAX_VALUE;

    private long retentionPeriodMillis = Long.MAX_VALUE;

    private Map<String, Supplier<Aggregator>> attachedAggregators = new HashMap<String, Supplier<Aggregator>>();

    public DataStreamConfig() {
    }

    public DataStreamConfig(String name) {
        setName(name);
    }

    public DataStreamConfig(DataStreamConfig defConfig) {
        this.name = defConfig.name;
        this.valueClass = defConfig.valueClass;
        this.initialRegionSize = defConfig.initialRegionSize;
        this.maxRegionSize = defConfig.maxRegionSize;
        this.maxRegionsPerPartition = defConfig.maxRegionsPerPartition;
        this.tenuringAgeMillis = defConfig.tenuringAgeMillis;
        this.indices = defConfig.indices;
        this.storageEnabled = defConfig.storageEnabled;
        this.storageDir = defConfig.storageDir;
    }

    public long getRetentionPeriodMillis() {
        return retentionPeriodMillis;
    }

    public DataStreamConfig setRetentionPeriodMillis(long retentionPeriodMillis) {
        this.retentionPeriodMillis = retentionPeriodMillis;
        return this;
    }

    public DataStreamConfig setRetentionPeriodMillis(long retentionPeriod, TimeUnit unit){
        return setRetentionPeriodMillis(unit.toMillis(retentionPeriod));
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

    public int getMaxRegionSize() {
        return maxRegionSize;
    }

    /**
     * Sets the maximum region size in bytes.
     *
     * @param maxRegionSize
     * @return this.
     * @throws IllegalArgumentException if maxRegionSize smaller than 1.
     */
    public DataStreamConfig setMaxRegionSize(long maxRegionSize) {
        if(maxRegionSize<0){
            throw new IllegalArgumentException();
        }

        if(maxRegionSize>Integer.MAX_VALUE){
            throw new IllegalArgumentException();
        }

        this.maxRegionSize = (int)maxRegionSize;
        return this;
    }

    public DataStreamConfig setMaxRegionSize(int maxRegionSize, MemoryUnit memoryUnit){
        return setMaxRegionSize((int)memoryUnit.toBytes(maxRegionSize));
    }

   // public DataStreamConfig setMaxRegionSize()

    /**
     * Gets the maximum number of retained regions per partition.
     *
     * @return the maximum number of retained regions per partition.
     */
    public int getMaxRegionsPerPartition() {
        return maxRegionsPerPartition;
    }

    /**
     * Sets the maximum number of regions per partition.
     *
     * If not explicitly configured, the default is Integer.MAX_VALUE.
     *
     * @param maxRegionsPerPartition
     * @return this
     * @throws IllegalArgumentException if maxRegionsPerPartition smaller than 1.
     */
    public DataStreamConfig setMaxRegionsPerPartition(int maxRegionsPerPartition) {
        this.maxRegionsPerPartition = checkPositive(maxRegionsPerPartition, "maxRegionsPerPartition should be larger than 0");
        return this;
    }

    public int getInitialRegionSize() {
        if(initialRegionSize==-1){
            return getMaxRegionSize();
        }
        return initialRegionSize;
    }

    /**
     * Sets the size in bytes for the initial region size.
     *
     * @param initialRegionSize
     * @return this
     * @throws IllegalArgumentException if initialRegionSize smaller than 1.
     */
    public DataStreamConfig setInitialRegionSize(int initialRegionSize) {
        this.initialRegionSize = checkPositive(initialRegionSize, "initialRegionSize should be larger than 0");
        return this;
    }

    /**
     * Gets the value class.
     *
     * @return the value class, or null if the value should be interpreted as a blob.
     */
    public Class getValueClass() {
        return valueClass;
    }

    /**
     * Sets the value class.
     *
     * If the value is set, the DataStream will analyze the content. If it isn't set, the
     * DataStream will assume it is a blob and will not try to understand.
     *
     * @param valueClass
     * @return
     */
    public DataStreamConfig setValueClass(Class valueClass) {
        this.valueClass = valueClass;
        return this;
    }

    /**
     * Sets the name of the data-stream.
     *
     * todo: probably we want to add constraints to the name so we can use the name as part of the filename
     * when writing to disk.
     *
     * @param name
     */
    public void setName(String name) {
        this.name = checkHasText(name,"name is empty or null");
    }

    /**
     * Gets the name of the data-stream.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the DataStreamConfig as a readonly configuration.
     *
     * @return
     */
    public DataStreamConfig getAsReadOnly() {
        //todo: we need to return an immutable wrapper.
        return this;
    }

    @Override
    public String toString() {
        return "DataStreamConfig{"
                + "name='" + name + '\''
                + ", valueClass=" + valueClass
                + ", initialRegionSize=" + initialRegionSize
                + ", maxRegionSize=" + maxRegionSize
                + ", maxRegionsPerPartition=" + maxRegionsPerPartition
                + ", indices=" + indices
                + ", tenuringAgeMillis=" + tenuringAgeMillis
                + ", attachedAggregators=" + attachedAggregators
                + '}';
    }
}
