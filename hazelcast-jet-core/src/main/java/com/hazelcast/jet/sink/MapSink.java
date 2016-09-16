/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sink;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.impl.dag.sink.MapPartitionWriter;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.runtime.DataWriter;

import java.util.List;

/**
 * A sink which uses a Hazelcast {@code IMap} as output.
 */
public class MapSink implements Sink {
    private final String name;

    /**
     * Constructs a sink with the given map name.
     *
     * @param name of the map to use as the output
     */
    public MapSink(String name) {
        this.name = name;
    }

    /**
     * Constructs a sink with the given list instance.
     *
     * @param map the map instance to be used as the output
     */
    public MapSink(IMap map) {
        this(map.getName());
    }

    @Override
    public DataWriter[] getWriters(JobContext jobContext) {
        List<Integer> localPartitions = JetUtil.getLocalPartitions(jobContext.getNodeEngine());
        DataWriter[] writers = new DataWriter[localPartitions.size()];
        for (int i = 0; i < localPartitions.size(); i++) {
            int partitionId = localPartitions.get(i);
            writers[i] = getPartitionWriter(jobContext, partitionId);
        }
        return writers;
    }

    protected DataWriter getPartitionWriter(JobContext jobContext, int partitionId) {
        return new MapPartitionWriter(jobContext, partitionId, name);
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "MapSink{"
                + "name='" + name + '\''
                + '}';
    }
}
