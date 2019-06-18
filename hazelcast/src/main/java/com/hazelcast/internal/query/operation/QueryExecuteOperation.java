package com.hazelcast.internal.query.operation;

import com.hazelcast.internal.query.QueryFragment;
import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.internal.query.worker.control.ExecuteControlTask;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.collection.PartitionIdSet;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryExecuteOperation extends QueryAbstractOperation {
    /** Unique query ID. */
    private QueryId queryId;

    /** Mapped ownership of partitions. */
    private Map<String, PartitionIdSet> partitionMapping;

    /** Rest fragments. */
    private List<QueryFragment> fragments;

    /** Arguments. */
    private List<Object> arguments;

    public QueryExecuteOperation() {
        // No-op.
    }

    public QueryExecuteOperation(QueryId queryId, Map<String, PartitionIdSet> partitionMapping,
        List<QueryFragment> fragments, List<Object> arguments) {
        this.queryId = queryId;
        this.partitionMapping = partitionMapping;
        this.fragments = fragments;
        this.arguments = arguments;
    }

    @Override
    public void run() throws Exception {
        QueryService svc = getService();

        ExecuteControlTask task = new ExecuteControlTask(queryId, partitionMapping, fragments, arguments);

        svc.onQueryExecuteRequest(task);
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public Map<String, PartitionIdSet> getPartitionMapping() {
        return partitionMapping;
    }

    public List<QueryFragment> getFragments() {
        return fragments;
    }

    public List<Object> getArguments() {
        return arguments;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(queryId);

        out.writeInt(partitionMapping.size());

        for (Map.Entry<String, PartitionIdSet> entry : partitionMapping.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().size());
            out.writeLongArray(entry.getValue().getBitSet().toLongArray());
        }

        out.writeObject(fragments);
        out.writeObject(arguments);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        queryId = in.readObject();

        int size = in.readInt();

        partitionMapping = new HashMap<>(size);

        for (int i = 0; i < size; i++)
            partitionMapping.put(in.readUTF(), new PartitionIdSet(in.readInt(), BitSet.valueOf(in.readLongArray())));

        fragments = in.readObject();
        arguments = in.readObject();
    }
}
