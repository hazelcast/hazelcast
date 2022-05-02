package com.hazelcast.tpc.engine.iouring;

import io.netty.channel.unix.Buffer;
import io.netty.incubator.channel.uring.Native;

import java.nio.ByteBuffer;

class AcceptMemory {
    final ByteBuffer memory;
    final long memoryAddress;
    final ByteBuffer lengthMemory;
    final long lengthMemoryAddress;

    AcceptMemory() {
        this.memory = Buffer.allocateDirectWithNativeOrder(Native.SIZEOF_SOCKADDR_STORAGE);
        this.memoryAddress = Buffer.memoryAddress(memory);
        this.lengthMemory = Buffer.allocateDirectWithNativeOrder(Long.BYTES);
        // Needs to be initialized to the size of acceptedAddressMemory.
        // See https://man7.org/linux/man-pages/man2/accept.2.html
        this.lengthMemory.putLong(0, Native.SIZEOF_SOCKADDR_STORAGE);
        this.lengthMemoryAddress = Buffer.memoryAddress(lengthMemory);
    }
}
