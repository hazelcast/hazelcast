package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;

/**
 * Responsible for writing a {@link com.hazelcast.nio.Packet} to a {@link java.nio.ByteBuffer}.
 * <p/>
 * The reason this abstraction exists, is that for Hazelcast Enterprise we can apply things like encryption.
 */
public interface PacketWriter {

    boolean writePacket(Packet packet, ByteBuffer socketBB);
}
