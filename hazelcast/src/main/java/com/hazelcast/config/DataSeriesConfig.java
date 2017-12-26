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

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 *
 */
public class DataSeriesConfig {
    public static final int KB = 1024;
    private String name;
    // todo: is there a good reason to make initial not equal to max?
    private long initialSegmentSize = KB * KB;
    private int maxSegmentSize = KB * KB;
    private int segmentsPerPartition = Integer.MAX_VALUE;
    private boolean isFixedLength;

    // to get rid of 'too' old segments.
    // so data is inserted on the segment as long as the tenuringAgeMillis has not been reached.
    // once it is reached, a new segment is created and this one is moved one to the back.
    // perhaps call it tenuring age?

    // if you want to do time based insertion, perhaps best to keep the maxSegmentSize as Long.MAX_VALUE?
    private long tenuringAgeMillis = Long.MAX_VALUE;

    public DataSeriesConfig() {
    }

    public DataSeriesConfig(String name) {
        this.name = name;
    }

    public DataSeriesConfig(DataSeriesConfig defConfig) {
        this.name = defConfig.name;
        this.initialSegmentSize = defConfig.initialSegmentSize;
        this.maxSegmentSize = defConfig.maxSegmentSize;
        this.segmentsPerPartition = defConfig.segmentsPerPartition;
        this.tenuringAgeMillis = defConfig.tenuringAgeMillis;
    }

    public long getTenuringAgeMillis() {
        return tenuringAgeMillis;
    }

    public DataSeriesConfig setTenuringAgeMillis(long tenuringAgeMillis) {
        this.tenuringAgeMillis = tenuringAgeMillis;
        return this;
    }

    public DataSeriesConfig setTenuringAge(int age, TimeUnit unit) {
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

    public DataSeriesConfig setMaxSegmentSize(int maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
        return this;
    }

    public int getSegmentsPerPartition() {
        return segmentsPerPartition;
    }

    public DataSeriesConfig setSegmentsPerPartition(int segmentsPerPartition) {
        this.segmentsPerPartition = segmentsPerPartition;
        return this;
    }

    public long getInitialSegmentSize() {
        return initialSegmentSize;
    }

    public DataSeriesConfig setInitialSegmentSize(long initialSegmentSize) {
        this.initialSegmentSize = checkPositive(initialSegmentSize, "initialSegmentSize should be larger than 0");
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public DataSeriesConfig getAsReadOnly() {
        return this;
    }

    @Override
    public String toString() {
        return "DataSeriesConfig{"
                + "name='" + name + '\''
                + ", initialSegmentSize=" + initialSegmentSize
                + ", maxSegmentSize=" + maxSegmentSize
                + ", segmentsPerPartition=" + segmentsPerPartition
                + ", tenuringAgeMillis=" + tenuringAgeMillis
                + '}';
    }
}
