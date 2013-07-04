package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;

import java.nio.ByteOrder;

/**
 * @author mdogan 6/15/13
 */
final class ByteBufferInputOutputFactory implements InputOutputFactory {

    private final ByteOrder byteOrder;

    ByteBufferInputOutputFactory(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
    }

    @Override
    public BufferObjectDataInput createInput(Data data, SerializationService service) {
        return new ByteBufferObjectDataInput(data, service, byteOrder);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        return new ByteBufferObjectDataInput(buffer, service, byteOrder);
    }

    @Override
    public BufferObjectDataOutput createOutput(int size, SerializationService service) {
        return new ByteBufferObjectDataOutput(size, service, byteOrder);
    }
}
