package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.impl.client.KeyBasedClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.SignalOperation;
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

public class SignalRequest extends KeyBasedClientRequest implements Portable, SecureRequest {

    private ObjectNamespace namespace;
    private String name;
    private long threadId;
    private String conditionId;
    private boolean all;

    public SignalRequest() {
    }

    public SignalRequest(ObjectNamespace namespace, String name, long threadId, String conditionId, boolean all) {
        this.namespace = namespace;
        this.name = name;
        this.threadId = threadId;
        this.conditionId = conditionId;
        this.all = all;
    }


    @Override
    protected Object getKey() {
        return name;
    }

    @Override
    protected Operation prepareOperation() {
        final Data key = serializationService.toData(name);
        return new SignalOperation(namespace, key, threadId, conditionId, all);
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
        return LockPortableHook.CONDITION_SIGNAL;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeUTF("cid", conditionId);
        writer.writeLong("tid", threadId);
        writer.writeBoolean("all", all);
        final ObjectDataOutput out = writer.getRawDataOutput();
        namespace.writeData(out);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        conditionId = reader.readUTF("cid");
        threadId = reader.readLong("tid");
        all = reader.readBoolean("all");
        final ObjectDataInput in = reader.getRawDataInput();
        namespace = new InternalLockNamespace();
        namespace.readData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        return new LockPermission(namespace.getObjectName(), ActionConstants.ACTION_LOCK);
    }
}
