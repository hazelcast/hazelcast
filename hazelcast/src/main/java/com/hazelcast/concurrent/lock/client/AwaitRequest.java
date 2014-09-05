package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.impl.client.KeyBasedClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.AwaitOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class AwaitRequest extends KeyBasedClientRequest implements Portable, SecureRequest {

    private ObjectNamespace namespace;
    private String name;
    private long timeout;
    private long threadId;
    private String conditionId;

    public AwaitRequest() {
    }

    public AwaitRequest(ObjectNamespace namespace, String name, long timeout, long threadId, String conditionId) {
        this.namespace = namespace;
        this.name = name;
        this.timeout = timeout;
        this.threadId = threadId;
        this.conditionId = conditionId;
    }

    @Override
    protected Object getKey() {
        return name;
    }

    @Override
    protected Operation prepareOperation() {
        final Data key = serializationService.toData(name);
        return new AwaitOperation(namespace, key, threadId, timeout, conditionId);
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return LockPortableHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return LockPortableHook.CONDITION_AWAIT;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeLong("tout", timeout);
        writer.writeLong("tid", threadId);
        writer.writeUTF("cid", conditionId);
        ObjectDataOutput out = writer.getRawDataOutput();
        namespace.writeData(out);

    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        timeout = reader.readLong("tout");
        threadId = reader.readLong("tid");
        conditionId = reader.readUTF("cid");
        ObjectDataInput in = reader.getRawDataInput();
        namespace = new InternalLockNamespace();
        namespace.readData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        return new LockPermission(namespace.getObjectName(), ActionConstants.ACTION_LOCK);
    }
}
