package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;

/**
 * @mdogan 6/15/13
 */
public interface InputOutputFactory {

    BufferObjectDataInput createInput(Data data, SerializationService service);

    BufferObjectDataInput createInput(byte[] buffer, SerializationService service);

    BufferObjectDataOutput createOutput(int size, SerializationService service);
}
