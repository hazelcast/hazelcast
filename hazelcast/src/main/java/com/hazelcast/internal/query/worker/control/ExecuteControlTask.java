package com.hazelcast.internal.query.worker.control;

import com.hazelcast.internal.query.QueryFragment;
import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryRootConsumer;
import com.hazelcast.util.collection.PartitionIdSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExecuteControlTask implements ControlTask {
    private final QueryId queryId;
    private final Map<String, PartitionIdSet> partitionMapping;
    private final List<QueryFragment> fragments;
    private final List<Object> arguments;
    private final QueryRootConsumer rootConsumer;

    public ExecuteControlTask(QueryId queryId, Map<String, PartitionIdSet> partitionMapping,
        List<QueryFragment> fragments, List<Object> arguments, QueryRootConsumer rootConsumer) {
        this.queryId = queryId;
        this.partitionMapping = partitionMapping;
        this.fragments = fragments;
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

    public List<QueryFragment> getFragments() {
        return fragments != null ? fragments : Collections.emptyList();
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public QueryRootConsumer getRootConsumer() {
        return rootConsumer;
    }

    @Override
    public String toString() {
        return "ExecuteControlTask{queryId=" + queryId + ", fragments=" + fragments + "}";
    }
}
