package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.concurrent.lock.GetLockOwnerOperation;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockPortableHook;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class GetLockOwnerRequest extends KeyBasedClientRequest implements Portable {

    private Data key;

    public GetLockOwnerRequest() {
    }

    public GetLockOwnerRequest(Data key) {
        this.key = key;
    }

    @Override
    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        String name = (String) getClientEngine().toObject(key);
        return new GetLockOwnerOperation(new InternalLockNamespace(name), key);
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
        return LockPortableHook.GET_LOCK_OWNER;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
    }

}
