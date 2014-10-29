package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Abstract Cache All Partitions request to handle InMemoryFormat which needed for operation provider
 */
abstract class AbstractCacheAllPartitionsRequest extends AllPartitionsClientRequest {

    protected String name;
    protected InMemoryFormat inMemoryFormat;

    protected AbstractCacheAllPartitionsRequest() {
    }

    protected AbstractCacheAllPartitionsRequest(String name, InMemoryFormat inMemoryFormat) {
        this.name = name;
        this.inMemoryFormat = inMemoryFormat;
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("i", inMemoryFormat.name());
    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
        inMemoryFormat = InMemoryFormat.valueOf(reader.readUTF("i"));

    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }
}
