package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;

/**
 * @mdogan 6/15/13
 */
final class ByteArrayInputOutputFactory implements InputOutputFactory {

    @Override
    public BufferObjectDataInput createInput(Data data, SerializationService service) {
        return new ByteArrayObjectDataInput(data, service);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        return new ByteArrayObjectDataInput(buffer, service);
    }

    @Override
    public BufferObjectDataOutput createOutput(int size, SerializationService service) {
        return new ByteArrayObjectDataOutput(size, service);
    }
}
