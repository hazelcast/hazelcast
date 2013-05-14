package com.hazelcast.concurrent.countdownlatch.client;

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.concurrent.countdownlatch.AwaitOperation;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchPortableHook;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @mdogan 5/14/13
 */

public final class AwaitRequest extends KeyBasedClientRequest implements Portable {

    private String name;
    private long timeout;

    public AwaitRequest() {
    }

    public AwaitRequest(String name, long timeout) {
        this.name = name;
        this.timeout = timeout;
    }

    @Override
    protected Object getKey() {
        return name;
    }

    @Override
    protected Operation prepareOperation() {
        return new AwaitOperation(name, timeout);
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CountDownLatchPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CountDownLatchPortableHook.AWAIT;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeLong("timeout", timeout);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        timeout = reader.readLong("timeout");
    }
}
