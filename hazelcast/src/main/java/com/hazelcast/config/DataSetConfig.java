package com.hazelcast.config;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.util.function.Supplier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

public class DataSetConfig {
    private String name;
    private Class keyClass;
    private Class valueClass;
    // todo: is there a good reason to make initial not equal to max?
    private long initialSegmentSize = 1024 * 1024;
    private int maxSegmentSize = 1024 * 1024;
    private int segmentsPerPartition = Integer.MAX_VALUE;
    private Set<String> indices = new HashSet<String>();

    // to get rid of 'too' old segments.
    // so data is inserted on the segment as long as the tenuringAgeMillis has not been reached.
    // once it is reached, a new segment is created and this one is moved one to the back.
    // perhaps call it tenuring age?

    // if you want to do time based insertion, perhaps best to keep the maxSegmentSize as Long.MAX_VALUE?
    private long tenuringAgeMillis = Long.MAX_VALUE;

    private Map<String, Supplier<Aggregator>> attachedAggregators = new HashMap<String, Supplier<Aggregator>>();

    public DataSetConfig() {
    }

    public DataSetConfig(String name) {
        this.name = name;
    }

    public DataSetConfig(DataSetConfig defConfig) {
        this.name = defConfig.name;
        this.keyClass = defConfig.keyClass;
        this.valueClass = defConfig.valueClass;
        this.initialSegmentSize = defConfig.initialSegmentSize;
        this.maxSegmentSize = defConfig.maxSegmentSize;
        this.segmentsPerPartition = defConfig.segmentsPerPartition;
        this.tenuringAgeMillis = defConfig.tenuringAgeMillis;
        this.indices = defConfig.indices;
    }

    public Map<String, Supplier<Aggregator>> getAttachedAggregators() {
        return attachedAggregators;
    }

    public DataSetConfig addAttachedAggregator(String id, Supplier<Aggregator> supplier) {
        this.attachedAggregators.put(id, supplier);
        return this;
    }

    public DataSetConfig addIndexField(String field) {
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

    public DataSetConfig setTenuringAgeMillis(long tenuringAgeMillis) {
        this.tenuringAgeMillis = tenuringAgeMillis;
        return this;
    }

    public DataSetConfig setTenuringAge(int age, TimeUnit unit) {
        return setTenuringAgeMillis(unit.toMillis(age));
    }

//    public int getMaxSegmentCount() {
//        return maxSegmentCount;
//    }
//
//    public DataSetConfig setMaxSegmentCount(int maxSegmentCount) {
//        this.maxSegmentCount = maxSegmentCount;
//        return this;
//    }

    public int getMaxSegmentSize() {
        return maxSegmentSize;
    }

    public DataSetConfig setMaxSegmentSize(int maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
        return this;
    }

    public int getSegmentsPerPartition() {
        return segmentsPerPartition;
    }

    public DataSetConfig setSegmentsPerPartition(int segmentsPerPartition) {
        this.segmentsPerPartition = segmentsPerPartition;
        return this;
    }

    public long getInitialSegmentSize() {
        return initialSegmentSize;
    }

    public DataSetConfig setInitialSegmentSize(long initialSegmentSize) {
        this.initialSegmentSize = checkPositive(initialSegmentSize, "initialSegmentSize should be larger than 0");
        return this;
    }

    public Class getKeyClass() {
        return keyClass;
    }

    public DataSetConfig setKeyClass(Class keyClass) {
        this.keyClass = checkNotNull(keyClass, "keyClass");
        return this;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public DataSetConfig setValueClass(Class valueClass) {
        this.valueClass = checkNotNull(valueClass, "valueClass");
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public DataSetConfig getAsReadOnly() {
        return this;
    }

    @Override
    public String toString() {
        return "DataSetConfig{" +
                "name='" + name + '\'' +
                ", keyClass=" + keyClass +
                ", valueClass=" + valueClass +
                ", initialSegmentSize=" + initialSegmentSize +
                ", maxSegmentSize=" + maxSegmentSize +
                ", segmentsPerPartition=" + segmentsPerPartition +
                ", indices=" + indices +
                ", tenuringAgeMillis=" + tenuringAgeMillis +
                ", attachedAggregators=" + attachedAggregators +
                '}';
    }
}
