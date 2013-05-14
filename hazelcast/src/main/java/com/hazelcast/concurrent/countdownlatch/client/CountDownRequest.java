package com.hazelcast.concurrent.countdownlatch.client;

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchPortableHook;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.countdownlatch.CountDownOperation;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @mdogan 5/14/13
 */

public final class CountDownRequest extends KeyBasedClientRequest implements Portable {

    private String name;

    public CountDownRequest() {
    }

    public CountDownRequest(String name) {
        this.name = name;
    }

    @Override
    protected Object getKey() {
        return name;
    }

    @Override
    protected Operation prepareOperation() {
        return new CountDownOperation(name);
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
        return CountDownLatchPortableHook.COUNT_DOWN;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
    }
}
