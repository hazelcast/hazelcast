package com.hazelcast.internal.query.operation;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.SqlService;

/**
 * Base class for all query operations.
 */
public abstract class QueryAbstractOperation extends Operation {
    @Override
    public String getServiceName() {
        return SqlService.SERVICE_NAME;
    }
}
