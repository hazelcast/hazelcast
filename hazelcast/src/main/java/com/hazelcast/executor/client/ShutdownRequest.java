package com.hazelcast.executor.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorPortableHook;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.security.Permission;

public class ShutdownRequest extends CallableClientRequest {

    private String name;

    public ShutdownRequest() {
    }

    public ShutdownRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() throws Exception {
        final DistributedExecutorService service = getService();
        service.shutdownExecutor(name);
        return true;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ExecutorPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ExecutorPortableHook.SHUTDOWN_REQUEST;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
