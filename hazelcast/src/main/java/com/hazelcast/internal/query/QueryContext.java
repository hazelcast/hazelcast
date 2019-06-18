package com.hazelcast.internal.query;

import com.hazelcast.internal.query.worker.control.FragmentDeployment;

import java.util.List;
import java.util.Map;

public class QueryContext {

    private final QueryService service;
    private final QueryId queryId;
    private final List<Object> arguments;

    /** Deployed fragments. */
    private final List<FragmentDeployment> fragmentDeployments;

    /** Maps an edge to array, whose length is stripe length, and values are data thread IDs. */
    private final Map<Integer, int[]> edgeToStripeMap;

    public QueryContext(QueryService service, QueryId queryId, List<Object> arguments,
        List<FragmentDeployment> fragmentDeployments, Map<Integer, int[]> edgeToStripeMap) {
        this.service = service;
        this.queryId = queryId;
        this.arguments = arguments;
        this.fragmentDeployments = fragmentDeployments;
        this.edgeToStripeMap = edgeToStripeMap;
    }

    public QueryService getService() {
        return service;
    }

    public QueryId getQueryId() {
        return queryId;
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
