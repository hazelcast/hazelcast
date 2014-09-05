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
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

/**
 * This class contains all configuration settings for a given map reduce job. This class is immutable and values
 * are set after the job itself is emitted to the job owner.
 */
public class JobTaskConfiguration {

    private final Address jobOwner;
    private final int chunkSize;
    private final String name;
    private final String jobId;
    private final Mapper mapper;
    private final CombinerFactory combinerFactory;
    private final ReducerFactory reducerFactory;
    private final KeyValueSource keyValueSource;
    private final NodeEngine nodeEngine;
    private final boolean communicateStats;
    private final TopologyChangedStrategy topologyChangedStrategy;

    //Deactivated checkstyle due to more than 10 parameters which is ok in this special case :)
    //I want that class to be immutable to utilize the memory effect of it.
    //CHECKSTYLE:OFF
    public JobTaskConfiguration(Address jobOwner, NodeEngine nodeEngine, int chunkSize, String name, String jobId, Mapper mapper,
                                CombinerFactory combinerFactory, ReducerFactory reducerFactory, KeyValueSource keyValueSource,
                                boolean communicateStats, TopologyChangedStrategy topologyChangedStrategy) {
        this.jobOwner = jobOwner;
        this.chunkSize = chunkSize;
        this.name = name;
        this.jobId = jobId;
        this.mapper = mapper;
        this.combinerFactory = combinerFactory;
        this.reducerFactory = reducerFactory;
        this.keyValueSource = keyValueSource;
        this.nodeEngine = nodeEngine;
        this.communicateStats = communicateStats;
        this.topologyChangedStrategy = topologyChangedStrategy;
    }
    //CHECKSTYLE:ON

    public Address getJobOwner() {
        return jobOwner;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

    public Mapper getMapper() {
        return mapper;
    }

    public CombinerFactory getCombinerFactory() {
        return combinerFactory;
    }

    public ReducerFactory getReducerFactory() {
        return reducerFactory;
    }

    public KeyValueSource getKeyValueSource() {
        return keyValueSource;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public boolean isCommunicateStats() {
        return communicateStats;
    }

    public TopologyChangedStrategy getTopologyChangedStrategy() {
        return topologyChangedStrategy;
    }

}
