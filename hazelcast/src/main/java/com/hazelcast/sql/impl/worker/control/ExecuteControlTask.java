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

package com.hazelcast.sql.impl.worker.control;

import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultConsumer;
import com.hazelcast.util.collection.PartitionIdSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Task to start query execution.
 */
public class ExecuteControlTask implements ControlTask {
    /** Query ID. */
    private final QueryId queryId;

    /** Partition mapping. */
    private final Map<String, PartitionIdSet> partitionMapping;

    /** Fragment descriptors. */
    private final List<QueryFragmentDescriptor> fragmentDescriptors;

    /** Outbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> outboundEdgeMap;

    /** Inbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> inboundEdgeMap;

    /** Query arguments. */
    private final List<Object> arguments;

    /** Root consumer (available only on initiating node). */
    private final QueryResultConsumer rootConsumer;

    public ExecuteControlTask(
        QueryId queryId,
        Map<String, PartitionIdSet> partitionMapping,
        List<QueryFragmentDescriptor> fragmentDescriptors,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        List<Object> arguments,
        QueryResultConsumer rootConsumer
    ) {
        this.queryId = queryId;
        this.partitionMapping = partitionMapping;
        this.fragmentDescriptors = fragmentDescriptors;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.arguments = arguments;
        this.rootConsumer = rootConsumer;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    public Map<String, PartitionIdSet> getPartitionMapping() {
        return partitionMapping;
    }

    public List<QueryFragmentDescriptor> getFragmentDescriptors() {
        return fragmentDescriptors != null ? fragmentDescriptors : Collections.emptyList();
    }

    public Map<Integer, Integer> getOutboundEdgeMap() {
        return outboundEdgeMap;
    }

    public Map<Integer, Integer> getInboundEdgeMap() {
        return inboundEdgeMap;
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public QueryResultConsumer getRootConsumer() {
        return rootConsumer;
    }
}
