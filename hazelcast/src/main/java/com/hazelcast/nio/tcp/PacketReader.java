package com.hazelcast.nio.tcp;

import java.nio.ByteBuffer;

/**
 * Responsible for reading packets from a {@link java.nio.ByteBuffer}.
 *
 * The reason this abstraction exists, is that for Hazelcast Enterprise we can apply things like encryption.
 */
public interface PacketReader {

    void readPacket(ByteBuffer inBuffer) throws Exception;
}
