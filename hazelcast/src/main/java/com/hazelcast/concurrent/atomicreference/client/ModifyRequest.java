package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.concurrent.atomicreference.AtomicReferencePortableHook;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.concurrent.atomicreference.SetOperation;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public abstract class ModifyRequest extends PartitionClientRequest implements Portable, SecureRequest {

    String name;
    Data update;

    public ModifyRequest() {
    }

    public ModifyRequest(String name, Data update) {
        this.name = name;
        this.update = update;
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
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return AtomicReferencePortableHook.F_ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        IOUtil.writeNullableData(out, update);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        ObjectDataInput in = reader.getRawDataInput();
        update = IOUtil.readNullableData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicReferencePermission(name, ActionConstants.ACTION_MODIFY);
    }
}
