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

package com.hazelcast.jet.source;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.impl.dag.source.MapPartitionProducer;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.JetUtil;
import java.util.List;

/**
 * A source which uses a Hazelcast {@code IMap} as the input.
 */
public class MapSource implements Source {

    private final String name;

    /**
     * Constructs a source with the given map name.
     *
     * @param name of the map to use as the input
     */
    public MapSource(String name) {
        this.name = name;
    }

    /**
     * Constructs a source with the given map.
     *
     * @param map the map instance to be used as the input
     */
    public MapSource(IMap map) {
        this(map.getName());
    }

    @Override
    public Producer[] getProducers(JobContext jobContext, Vertex vertex) {
        List<Integer> localPartitions = JetUtil.getLocalPartitions(jobContext.getNodeEngine());
        Producer[] readers = new Producer[localPartitions.size()];
        for (int i = 0; i < localPartitions.size(); i++) {
            int partitionId = localPartitions.get(i);
            readers[i] = getReader(jobContext, partitionId);
        }
        return readers;
    }

    protected Producer getReader(JobContext jobContext, int partitionId) {
        return new MapPartitionProducer(jobContext, name, partitionId);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "MapSource{"
                + "name='" + name + '\''
                + '}';
    }
}
