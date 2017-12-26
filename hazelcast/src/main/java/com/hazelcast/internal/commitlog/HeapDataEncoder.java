package com.hazelcast.internal.commitlog;

import com.hazelcast.internal.memory.impl.AlignmentAwareMemoryAccessor;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import sun.misc.Unsafe;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * An encoder that reads/writes variable length {@link HeapData}.
 * <p>
 * So if you want to writes blobs of data, this is your friend.
 */
public class HeapDataEncoder extends Encoder {

    private static final AlignmentAwareMemoryAccessor memoryAccessor = AlignmentAwareMemoryAccessor.INSTANCE;

    public HeapDataEncoder() {
    }

    public HeapDataEncoder(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public HeapData load() {
        int size = memoryAccessor.getInt(dataAddress + dataOffset);
        byte[] bytes = new byte[size];
        dataOffset += INT_SIZE_IN_BYTES;
        memoryAccessor.copyMemory(null, dataAddress + dataOffset, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
        dataOffset += size;
        return new HeapData(bytes);
    }

    @Override
    public boolean store(Object object) {
        HeapData data = serializationService.toData(object);
        byte[] bytes = data.toByteArray();
        //System.out.println("store size:"+bytes.length);
        int remaining = dataLength - dataOffset;
        if (remaining < bytes.length + INT_SIZE_IN_BYTES) {
            return false;
        }
        memoryAccessor.putInt(dataAddress + dataOffset, bytes.length);
        dataOffset += INT_SIZE_IN_BYTES;
        memoryAccessor.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, dataAddress + dataOffset, bytes.length);
        dataOffset += bytes.length;
        return true;
    }
}
