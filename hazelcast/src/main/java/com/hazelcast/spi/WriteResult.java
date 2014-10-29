package com.hazelcast.spi;

/**
 * The result of writing a Packet to a {@link com.hazelcast.nio.Connection}.
 *
 * This functionality is needed for back pressure.
 */
public enum WriteResult {
    /**
     * If the packet was written successfully.
     */
    SUCCESS,

    /**
     * If the packet failed to be written, e.g. because the connection is closed.
     */
    FAILURE,

    /**
     * This can mean multiple things; first of all it means that the connection is 'full' and in theory there is no more room
     * to write the a packet. But it depends on the call if the packet is still accepted.
     *
     * For backup operations we need to write the result because we can't apply back pressure on the backup-operation (we would
     * be stalling the partition-thread).
     */
    FULL
}
