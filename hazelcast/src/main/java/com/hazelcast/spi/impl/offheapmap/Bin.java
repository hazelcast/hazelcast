package com.hazelcast.spi.impl.offheapmap;

import com.hazelcast.spi.impl.engine.frame.Frame;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static io.netty.channel.unix.Buffer.memoryAddress;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class Bin {

    private Frame frame;
    private ByteBuffer buffer;
    private int dataOffset;
    private int size;

    // for debugging only because it creates litter!
    public byte[] bytes() {
        byte[] bytes = new byte[size];
        int pos = buffer.position();
        buffer.position(dataOffset);
        buffer.get(bytes);
        buffer.position(pos);
        return bytes;
    }

    public void init(Frame frame) {
        this.frame = frame;
        this.buffer = frame.byteBuffer();
        this.size = frame.readInt();
        this.dataOffset = frame.position();
        frame.incPosition(size);
    }

    // Todo: very inefficient.
    public boolean unsafeEquals(Unsafe unsafe, long address) {
        int keyLength = unsafe.getInt(address);
        if (keyLength != size) {
            return false;
        }

        address += BYTES_INT;

        ByteBuffer buffer = frame.byteBuffer();
        for (int k = 0; k < size; k++) {
            if (buffer.get(dataOffset + k) != unsafe.getByte(address + k)) {
                return false;
            }
        }

        return true;
    }

    public int hash() {
        int result = 1;
        for (int k = 0; k < size; k++) {
            result = 31 * result + buffer.get(dataOffset + k);
        }

        return result;
    }

    /**
     * Copies the content of Bin to offheap.
     *
     * @param unsafe
     * @param dstAddress
     */
    public void copyTo(Unsafe unsafe, long dstAddress) {
        unsafe.putInt(dstAddress, size);
        dstAddress += INT_SIZE_IN_BYTES;

        ByteBuffer buffer = frame.byteBuffer();
        if (buffer.hasArray()) {
            unsafe.copyMemory(buffer.array(), ARRAY_BYTE_BASE_OFFSET + dataOffset, null, dstAddress, size);
        } else {
            unsafe.copyMemory(memoryAddress(buffer) + dataOffset, dstAddress, size);
        }
    }

    /**
     * The number of bytes taken up by the key itself (excluding the size field).
     *
     * @return
     */
    public int size() {
        return size;
    }

    public void clear() {
        this.frame = null;
    }
}
