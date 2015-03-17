package com.hazelcast.spi.impl.proxyservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.io.IOException;

public class DistributedObjectDestroyOperation
        extends AbstractOperation {

    private String serviceName;
    private String name;

    public DistributedObjectDestroyOperation() {
    }

    public DistributedObjectDestroyOperation(String serviceName, String name) {
        this.serviceName = serviceName;
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ProxyServiceImpl proxyService = getService();
        proxyService.destroyLocalDistributedObject(serviceName, name, false);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(serviceName);
        out.writeObject(name);
        // writing as object for backward-compatibility
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readUTF();
        name = in.readObject();
    }
}
