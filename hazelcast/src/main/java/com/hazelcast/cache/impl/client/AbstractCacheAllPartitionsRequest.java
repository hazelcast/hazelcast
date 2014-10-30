package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Abstract Cache All Partitions request to handle InMemoryFormat which needed for operation provider
 */
abstract class AbstractCacheAllPartitionsRequest extends AllPartitionsClientRequest {

    protected String name;

    protected AbstractCacheAllPartitionsRequest() {
    }

    protected AbstractCacheAllPartitionsRequest(String name) {
        this.name = name;
    }

    CacheOperationProvider getOperationProvider() {
        ICacheService service = getService();
        CacheConfig cacheConfig = service.getCacheConfig(name);
        return service.getCacheOperationProvider(name, cacheConfig.getInMemoryFormat());
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");

    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }
}
