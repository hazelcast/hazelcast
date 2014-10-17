package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;

/**
 * TODO add a proper JavaDoc
 */
public interface CacheOperationProvider {

    Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get);

    Operation createGetOperation(Data key, ExpiryPolicy policy);
}
