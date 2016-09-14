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

import com.hazelcast.core.IList;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.dag.source.ListPartitionReader;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.NodeEngine;

/**
 * A source which uses a Hazelcast {@code IList} as the input.
 */
public class ListSource implements Source {

    private final String name;

    /**
     * Constructs a source with the given list name.
     *
     * @param name of the list to use as the input
     */
    public ListSource(String name) {
        this.name = name;
    }

    /**
     * Constructs a source with the given list.
     *
     * @param list the list instance to be used as the input
     */
    public ListSource(IList list) {
        this(list.getName());
    }

    @Override
    public Producer[] getProducers(JobContext jobContext, Vertex vertex) {
        NodeEngine engine = jobContext.getNodeEngine();
        Data nameAsData = engine.getSerializationService().toData(name, StringPartitioningStrategy.INSTANCE);
        int partitionId = engine.getPartitionService().getPartitionId(nameAsData);
        return JetUtil.isPartitionLocal(engine, partitionId)
                ? new Producer[] { new ListPartitionReader(jobContext, name, partitionId) }
                : new Producer[0];
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "ListSource{name='" + name + "'}";
    }
}
