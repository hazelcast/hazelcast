package com.hazelcast.internal.query.operation;

import com.hazelcast.internal.query.QueryService;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Base class for all query operations.
 */
public abstract class QueryAbstractOperation extends Operation {
    @Override
    public String getServiceName() {
        return QueryService.SERVICE_NAME;
    }
}
