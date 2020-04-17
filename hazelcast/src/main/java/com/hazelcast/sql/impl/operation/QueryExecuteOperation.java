/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Operation which is broadcasted to participating members to start query execution.
 */
public class QueryExecuteOperation extends QueryAbstractIdAwareOperation {
    /** Mapped ownership of partitions. We also piggyback on this collection to get participant IDs. */
    private Map<UUID, PartitionIdSet> partitionMap;

    /** Fragment descriptors. */
    private List<QueryExecuteOperationFragment> fragments;

    /** Outbound edge mapping (from edge ID to owning fragment position). */
    private Map<Integer, Integer> outboundEdgeMap;

    /** Inbound edge mapping (from edge ID to owning fragment position). */
    private Map<Integer, Integer> inboundEdgeMap;

    /** Map from edge ID to initial memory assigned to senders. */
    private Map<Integer, Long> edgeInitialMemoryMap;

    private List<Object> arguments;

    /** Root fragment result consumer. Applicable only to root fragment being executed on initiator. */
    private transient RootResultConsumer rootConsumer;
    private transient int rootBatchSize;

    public QueryExecuteOperation() {
        // No-op.
    }

    public QueryExecuteOperation(
        QueryId queryId,
        Map<UUID, PartitionIdSet> partitionMap,
        List<QueryExecuteOperationFragment> fragments,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        Map<Integer, Long> edgeInitialMemoryMap,
        List<Object> arguments
    ) {
        super(queryId);

        assert partitionMap != null && !partitionMap.isEmpty() : partitionMap;
        assert fragments != null && fragments.size() > 0 : fragments;
        assert outboundEdgeMap != null;
        assert inboundEdgeMap != null;
        assert inboundEdgeMap.size() == outboundEdgeMap.size();
        assert edgeInitialMemoryMap != null;
        assert edgeInitialMemoryMap.size() == outboundEdgeMap.size();

        this.queryId = queryId;
        this.partitionMap = partitionMap;
        this.fragments = fragments;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.edgeInitialMemoryMap = edgeInitialMemoryMap;
        this.arguments = arguments;
    }

    public Map<UUID, PartitionIdSet> getPartitionMap() {
        return partitionMap;
    }

    public List<QueryExecuteOperationFragment> getFragments() {
        return fragments;
    }

    public Map<Integer, Integer> getOutboundEdgeMap() {
        return outboundEdgeMap;
    }

    public Map<Integer, Integer> getInboundEdgeMap() {
        return inboundEdgeMap;
    }

    public Map<Integer, Long> getEdgeInitialMemoryMap() {
        return edgeInitialMemoryMap;
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public RootResultConsumer getRootConsumer() {
        return rootConsumer;
    }

    public int getRootBatchSize() {
        return rootBatchSize;
    }

    public QueryExecuteOperation setRootConsumer(RootResultConsumer rootConsumer, int rootBatchSize) {
        this.rootConsumer = rootConsumer;
        this.rootBatchSize = rootBatchSize;

        return this;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.OPERATION_EXECUTE;
    }

    @Override
    protected void writeInternal1(ObjectDataOutput out) throws IOException {
        // Write partitions.
        out.writeInt(partitionMap.size());

        for (Map.Entry<UUID, PartitionIdSet> entry : partitionMap.entrySet()) {
            UUIDSerializationUtil.writeUUID(out, entry.getKey());
            SerializationUtil.writeNullablePartitionIdSet(entry.getValue(), out);
        }

        // Write fragments.
        out.writeInt(fragments.size());

        for (QueryExecuteOperationFragment fragment : fragments) {
            out.writeObject(fragment);
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

        out.writeInt(edgeInitialMemoryMap.size());

        for (Map.Entry<Integer, Long> entry : edgeInitialMemoryMap.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeLong(entry.getValue());
        }

        // Write arguments.
        if (arguments == null) {
            out.writeInt(0);
        } else {
            out.writeInt(arguments.size());

            for (Object argument : arguments) {
                out.writeObject(argument);
            }
        }
    }

    @Override
    protected void readInternal1(ObjectDataInput in) throws IOException {
        // Read partitions.
        int partitionMappingCnt = in.readInt();

        partitionMap = new HashMap<>(partitionMappingCnt);

        for (int i = 0; i < partitionMappingCnt; i++) {
            UUID key = UUIDSerializationUtil.readUUID(in);
            PartitionIdSet val = SerializationUtil.readNullablePartitionIdSet(in);

            partitionMap.put(key, val);
        }

        // Read fragments.
        int fragmentCnt = in.readInt();

        fragments = new ArrayList<>(fragmentCnt);

        for (int i = 0; i < fragmentCnt; i++) {
            QueryExecuteOperationFragment fragment = in.readObject();

            fragments.add(fragment);
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

        int edgeCreditMapSize = in.readInt();

        edgeInitialMemoryMap = new HashMap<>(edgeCreditMapSize);

        for (int i = 0; i < edgeCreditMapSize; i++) {
            edgeInitialMemoryMap.put(in.readInt(), in.readLong());
        }

        // Read arguments.
        int argumentCnt = in.readInt();

        if (argumentCnt == 0) {
            arguments = Collections.emptyList();
        } else {
            arguments = new ArrayList<>(argumentCnt);

            for (int i = 0; i < argumentCnt; i++) {
                arguments.add(in.readObject());
            }
        }
    }
}
