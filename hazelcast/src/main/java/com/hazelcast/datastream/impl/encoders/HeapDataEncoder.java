package com.hazelcast.datastream.impl.encoders;

import com.hazelcast.internal.serialization.impl.HeapData;
import sun.misc.Unsafe;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * An encoder that reads/writes variable length heapdata.
 * <p>
 * todo: we shuld
 * <p>
 * So if you want to writes blobs of data, this is your friend.
 */
public class HeapDataEncoder extends DSEncoder {

    @Override
    public HeapData load() {
        // the first 4 bytes are the length
        int size = unsafe.getInt(dataAddress + dataOffset);
        //System.out.println("load size:"+size);
        byte[] bytes = new byte[size];
        dataOffset += INT_SIZE_IN_BYTES;
        unsafe.copyMemory(null, dataAddress + dataOffset, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
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
        unsafe.putInt(dataAddress + dataOffset, bytes.length);
        dataOffset += INT_SIZE_IN_BYTES;

        unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, dataAddress + dataOffset, bytes.length);

        dataOffset += bytes.length;
        return true;
    }
}
