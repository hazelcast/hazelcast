package com.hazelcast.internal.query;

import com.hazelcast.internal.query.worker.control.FragmentDeployment;
import com.hazelcast.spi.NodeEngine;

import java.util.List;
import java.util.Map;

public class QueryContext {

    private final NodeEngine nodeEngine;
    private final QueryId queryId;
    private final List<Object> arguments;
    private final QueryResultConsumer rootConsumer;

    /** Deployed fragments. */
    private final List<FragmentDeployment> fragmentDeployments;

    /** Maps an edge to array, whose length is stripe length, and values are data thread IDs. */
    private final Map<Integer, int[]> edgeToStripeMap;

    public QueryContext(NodeEngine nodeEngine, QueryId queryId, List<Object> arguments, QueryResultConsumer rootConsumer,
        List<FragmentDeployment> fragmentDeployments, Map<Integer, int[]> edgeToStripeMap) {
        this.nodeEngine = nodeEngine;
        this.queryId = queryId;
        this.arguments = arguments;
        this.rootConsumer = rootConsumer;
        this.fragmentDeployments = fragmentDeployments;
        this.edgeToStripeMap = edgeToStripeMap;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public QueryResultConsumer getRootConsumer() {
        return rootConsumer;
    }

    public List<FragmentDeployment> getFragmentDeployments() {
        return fragmentDeployments;
    }

    public Map<Integer, int[]> getEdgeToStripeMap() {
        return edgeToStripeMap;
    }

    public int argumentCount() {
        return arguments != null ? arguments.size() : 0;
    }

    public Object arguments(int idx) {
        if (idx >= argumentCount())
            throw new IllegalArgumentException("Parameter not found: " + idx);

        return arguments.get(idx);
    }
}
