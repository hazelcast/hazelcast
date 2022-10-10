package com.hazelcast.internal.tpc.iouring;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.internal.tpc.iouring.Linux.SIZEOF_SOCKADDR_STORAGE;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;

// There will only be 1 accept request at any given moment in the system
// So we don't need to worry about concurrent access to the same AcceptMemory.
public class AcceptMemory {
    public final ByteBuffer memory;
    public final long memoryAddress;
    public final ByteBuffer lengthMemory;
    public final long lengthMemoryAddress;

    public AcceptMemory() {
        this.memory = ByteBuffer.allocateDirect(SIZEOF_SOCKADDR_STORAGE);
        memory.order(ByteOrder.nativeOrder());
        this.memoryAddress = addressOf(memory);

        this.lengthMemory = ByteBuffer.allocateDirect(SIZEOF_LONG);
        lengthMemory.order(ByteOrder.nativeOrder());

        // Needs to be initialized to the size of acceptedAddressMemory.
        // See https://man7.org/linux/man-pages/man2/accept.2.html
        this.lengthMemory.putLong(0, SIZEOF_SOCKADDR_STORAGE);
        this.lengthMemoryAddress = addressOf(lengthMemory);
    }
}
