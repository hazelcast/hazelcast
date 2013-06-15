package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;

/**
 * @mdogan 6/15/13
 */
final class UnsafeInputOutputFactory implements InputOutputFactory {

    @Override
    public BufferObjectDataInput createInput(Data data, SerializationService service) {
        return new UnsafeObjectDataInput(data, service);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        return new UnsafeObjectDataInput(buffer, service);
    }

    @Override
    public BufferObjectDataOutput createOutput(int size, SerializationService service) {
        return new UnsafeObjectDataOutput(size, service);
    }

    static boolean unsafeAvailable() {
        try {
            final long l = UnsafeHelper.BYTE_ARRAY_BASE_OFFSET;
            return l > -1;
        } catch (Throwable ignored) {
        }
        return false;
    }
}
