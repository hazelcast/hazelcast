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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultConsumer;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.worker.control.ExecuteControlTask;
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
public class QueryExecuteOperation extends QueryAbstractOperation {
    /** Unique query ID. */
    private QueryId queryId;

    /** Member IDs. */
    private List<String> ids;

    /** Mapped ownership of partitions. */
    private Map<String, PartitionIdSet> partitionMapping;

    /** Fragments. */
    private List<QueryFragment> fragments;

    /** Arguments. */
    private List<Object> arguments;

    /** Seed used for load balancing. */
    private int seed;

    /** Result consumer. */
    private transient QueryResultConsumer rootConsumer;

    public QueryExecuteOperation() {
        // No-op.
    }

    public QueryExecuteOperation(
        QueryId queryId,
        List<String> ids,
        Map<String, PartitionIdSet> partitionMapping,
        List<QueryFragment> fragments,
        List<Object> arguments,
        int seed,
        QueryResultConsumer rootConsumer
    ) {
        assert fragments != null;

        this.queryId = queryId;
        this.ids = ids;
        this.partitionMapping = partitionMapping;
        this.fragments = fragments;
        this.arguments = arguments;
        this.seed = seed;
        this.rootConsumer = rootConsumer;
    }

    @Override
    public void run() throws Exception {
        SqlServiceImpl svc = getSqlService();

        svc.onQueryExecuteRequest(getTask());
    }

    public ExecuteControlTask getTask() {
        return new ExecuteControlTask(queryId, ids, partitionMapping, fragments, arguments, seed, rootConsumer);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        // Write query ID.
        queryId.writeData(out);

        // Write addresses.
        out.writeInt(ids.size());

        for (String id : ids)
            out.writeUTF(id);

        // Write partitions.
        out.writeInt(partitionMapping.size());

        for (Map.Entry<String, PartitionIdSet> entry : partitionMapping.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().size());
            out.writeLongArray(entry.getValue().getBitSet().toLongArray());
        }

        // Write fragments.
        out.writeInt(fragments.size());

        for (QueryFragment fragment : fragments)
            fragment.writeData(out);

        // Write arguments.
        if (arguments == null)
            out.writeInt(0);
        else {
            out.writeInt(arguments.size());

            for (Object argument : arguments)
                out.writeObject(argument);
        }

        out.writeInt(seed);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        // Read query ID.
        queryId = new QueryId();
        queryId.readData(in);

        // Read IDs.
        int idsCnt = in.readInt();

        ids = new ArrayList<>(idsCnt);

        for (int i = 0; i < idsCnt; i++)
            ids.add(in.readUTF());

        // Read partitions.
        int partitionMappingCnt = in.readInt();

        partitionMapping = new HashMap<>(partitionMappingCnt);

        for (int i = 0; i < partitionMappingCnt; i++)
            partitionMapping.put(in.readUTF(), new PartitionIdSet(in.readInt(), BitSet.valueOf(in.readLongArray())));

        // Read fragments.
        int fragmentCnt = in.readInt();

        fragments = new ArrayList<>(fragmentCnt);

        for (int i = 0; i < fragmentCnt; i++) {
            QueryFragment fragment = new QueryFragment();

            fragment.readData(in);

            fragments.add(fragment);
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

        seed = in.readInt();
    }
}
