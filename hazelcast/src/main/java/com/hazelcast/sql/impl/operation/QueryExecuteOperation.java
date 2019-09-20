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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.util.collection.PartitionIdSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Operation which is broadcast to participating members to start query execution.
 */
public class QueryExecuteOperation extends QueryOperation {
    /** Unique query ID. */
    private QueryId queryId;

    /** Mapped ownership of partitions. */
    private Map<String, PartitionIdSet> partitionMapping;

    /** Fragment descriptors. */
    private List<QueryFragmentDescriptor> fragmentDescriptors;

    /** Outbound edge mapping (from edge ID to owning fragment position). */
    private Map<Integer, Integer> outboundEdgeMap;

    /** Inbound edge mapping (from edge ID to owning fragment position). */
    private Map<Integer, Integer> inboundEdgeMap;

    /** Arguments. */
    private List<Object> arguments;

    /** Offset which defines which data thread will be used for fragments. */
    private int baseDeploymentOffset;

    public QueryExecuteOperation() {
        // No-op.
    }

    public QueryExecuteOperation(
        QueryId queryId,
        Map<String, PartitionIdSet> partitionMapping,
        List<QueryFragmentDescriptor> fragmentDescriptors,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        List<Object> arguments,
        int baseDeploymentOffset
    ) {
        this.queryId = queryId;
        this.partitionMapping = partitionMapping;
        this.fragmentDescriptors = fragmentDescriptors;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.arguments = arguments;
        this.baseDeploymentOffset = baseDeploymentOffset;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public Map<String, PartitionIdSet> getPartitionMapping() {
        return partitionMapping;
    }

    public List<QueryFragmentDescriptor> getFragmentDescriptors() {
        return fragmentDescriptors;
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

    public int getBaseDeploymentOffset() {
        return baseDeploymentOffset;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        // Write query ID.
        queryId.writeData(out);

        // Write partitions.
        out.writeInt(partitionMapping.size());

        for (Map.Entry<String, PartitionIdSet> entry : partitionMapping.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().size());
            out.writeLongArray(entry.getValue().getBitSet().toLongArray());
        }

        // Write fragments.
        out.writeInt(fragmentDescriptors.size());

        for (QueryFragmentDescriptor fragmentDescriptor : fragmentDescriptors) {
            out.writeObject(fragmentDescriptor);
        }

        // Write edge mappings.
        out.writeInt(outboundEdgeMap.size());

        for (Map.Entry<Integer, Integer> entry : outboundEdgeMap.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue());
        }

        out.writeInt(inboundEdgeMap.size());

        for (Map.Entry<Integer, Integer> entry : inboundEdgeMap.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeInt(entry.getValue());
        }

        // Write arguments.
        if (arguments == null)
            out.writeInt(0);
        else {
            out.writeInt(arguments.size());

            for (Object argument : arguments)
                out.writeObject(argument);
        }

        // Write deployment offset.
        out.writeInt(baseDeploymentOffset);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        // Read query ID.
        queryId = new QueryId();
        queryId.readData(in);

        // Read partitions.
        int partitionMappingCnt = in.readInt();

        partitionMapping = new HashMap<>(partitionMappingCnt);

        for (int i = 0; i < partitionMappingCnt; i++)
            partitionMapping.put(in.readUTF(), new PartitionIdSet(in.readInt(), BitSet.valueOf(in.readLongArray())));

        // Read fragments.
        int fragmentCnt = in.readInt();

        fragmentDescriptors = new ArrayList<>(fragmentDescriptors);

        for (int i = 0; i < fragmentCnt; i++) {
            fragmentDescriptors.add(in.readObject());
        }

        // Read edge mappings.
        int outboundEdgeMapSize = in.readInt();

        outboundEdgeMap = new HashMap<>(outboundEdgeMapSize);

        for (int i = 0; i < outboundEdgeMapSize; i++) {
            outboundEdgeMap.put(in.readInt(), in.readInt());
        }

        int inboundEdgeMapSize = in.readInt();

        inboundEdgeMap = new HashMap<>(inboundEdgeMapSize);

        for (int i = 0; i < inboundEdgeMapSize; i++) {
            inboundEdgeMap.put(in.readInt(), in.readInt());
        }

        // Read arguments.
        int argumentCnt = in.readInt();

        if (argumentCnt == 0)
            arguments = Collections.emptyList();
        else {
            arguments = new ArrayList<>(argumentCnt);

            for (int i = 0; i < argumentCnt; i++)
                arguments.add(in.readObject());
        }

        // Read deployment offset.
        baseDeploymentOffset = in.readInt();
    }
}
