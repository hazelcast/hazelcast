package com.hazelcast.collection.operations.client;

import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.concurrent.lock.client.AbstractUnlockRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

/**
 * @ali 5/23/13
 */
public class MultiMapUnlockRequest extends AbstractUnlockRequest {

    CollectionProxyId proxyId;

    public MultiMapUnlockRequest() {
    }

    public MultiMapUnlockRequest(Data key, int threadId, CollectionProxyId proxyId) {
        super(key, threadId);
        this.proxyId = proxyId;
    }

    public MultiMapUnlockRequest(Data key, int threadId, boolean force, CollectionProxyId proxyId) {
        super(key, threadId, force);
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
        return CollectionPortableHook.UNLOCK;
    }
}
