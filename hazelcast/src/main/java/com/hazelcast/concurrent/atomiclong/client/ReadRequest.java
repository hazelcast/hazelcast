package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.concurrent.atomiclong.AtomicLongPortableHook;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;

import java.io.IOException;
import java.security.Permission;

public abstract class ReadRequest extends PartitionClientRequest implements Portable, SecureRequest {

    String name;

    public ReadRequest() {
    }

    public ReadRequest(String name) {
        this.name = name;
    }

    @Override
    protected int getPartition() {
        Data key = getClientEngine().getSerializationService().toData(name);
        return getClientEngine().getPartitionService().getPartitionId(key);
    }

    @Override
    protected int getReplicaIndex() {
        return 0;
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return AtomicLongPortableHook.F_ID;
    }

     @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicLongPermission(name, ActionConstants.ACTION_READ);
    }
}