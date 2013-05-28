package com.hazelcast.collection.operations.client;

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.concurrent.lock.client.AbstractIsLockedRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

/**
 * @ali 5/23/13
 */
public class MultiMapIsLockedRequest extends AbstractIsLockedRequest implements RetryableRequest {

    CollectionProxyId proxyId;

    public MultiMapIsLockedRequest() {
    }

    public MultiMapIsLockedRequest(Data key, CollectionProxyId proxyId) {
        super(key);
        this.proxyId = proxyId;
    }

    protected ObjectNamespace getNamespace() {
        return new DefaultObjectNamespace(CollectionService.SERVICE_NAME, proxyId);
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        proxyId.writeData(writer.getRawDataOutput());
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        proxyId = new CollectionProxyId();
        proxyId.readData(reader.getRawDataInput());
    }


    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.IS_LOCKED;
    }
}
