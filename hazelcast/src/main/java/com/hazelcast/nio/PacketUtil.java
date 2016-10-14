/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio;

import com.hazelcast.internal.serialization.InternalSerializationService;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.readInt;
import static com.hazelcast.nio.Bits.readShort;
import static com.hazelcast.nio.Bits.writeInt;
import static com.hazelcast.nio.Bits.writeShort;

public final class PacketUtil {

    private static final int OFFSET_VERSION = 0;
    private static final int OFFSET_FLAGS = OFFSET_VERSION + 1;
    private static final int OFFSET_PARTITION_ID = OFFSET_FLAGS + SHORT_SIZE_IN_BYTES;
    private static final int OFFSET_SIZE = OFFSET_PARTITION_ID + INT_SIZE_IN_BYTES;
    private static final int OFFSET_PAYLOAD = OFFSET_SIZE + INT_SIZE_IN_BYTES;

    private PacketUtil() {
    }

    public static Packet toPacket(byte[] bytes) {
        byte[] payload = new byte[bytes.length - OFFSET_PAYLOAD];
        System.arraycopy(bytes, OFFSET_PAYLOAD, payload, 0, payload.length);

        short flags = readShort(bytes, OFFSET_FLAGS, true);
        int partitionId = readInt(bytes, OFFSET_PARTITION_ID, true);
        return new Packet(payload, partitionId)
                .setAllFlags(flags);
    }

    public static byte[] getPayload(byte[] packet) {
        byte[] payload = new byte[packet.length - OFFSET_PAYLOAD];
        System.arraycopy(packet, OFFSET_PAYLOAD, payload, 0, payload.length);
        return payload;
    }

    public static byte[] fromPacket(Packet packet) {
        byte[] payload = packet.toByteArray();
        byte[] bytes = new byte[OFFSET_PAYLOAD + payload.length];
        System.arraycopy(payload, 0, bytes, OFFSET_PAYLOAD, payload.length);

        // version
        bytes[OFFSET_VERSION] = Packet.VERSION;
        //flags
        writeShort(bytes, OFFSET_FLAGS, packet.getFlags(), true);
        //partition-id
        writeInt(bytes, OFFSET_PARTITION_ID, packet.getPartitionId(), true);
        //size
        writeInt(bytes, OFFSET_SIZE, payload.length, true);

        return bytes;
    }

    public static byte[] toBytePacket(InternalSerializationService serializationService,
                                      Object payload,
                                      int flags,
                                      int partitionId) {
        byte[] bytes = serializationService.toBytes(OFFSET_PAYLOAD, payload);
        // version
        bytes[OFFSET_VERSION] = Packet.VERSION;
        //flags
        writeShort(bytes, OFFSET_FLAGS, (short) flags, true);
        //partition-id
        writeInt(bytes, OFFSET_PARTITION_ID, partitionId, true);
        //size
        writeInt(bytes, OFFSET_SIZE, bytes.length - OFFSET_PAYLOAD, true);

        return bytes;
    }

    public static short getFlags(byte[] packet) {
        return readShort(packet, OFFSET_FLAGS, true);
    }

    public static int getSize(byte[] packet) {
        return readInt(packet, OFFSET_SIZE, true);
    }

    public static int getPartition(byte[] packet) {
        return readInt(packet, OFFSET_PARTITION_ID, true);
    }


    /**
     * Checks if a flag is set.
     *
     * @param flag the flag to check
     * @return true if the flag is set, false otherwise.
     */
    public static boolean isFlagSet(byte[] packet, int flag) {
        return (getFlags(packet) & flag) != 0;
    }

}
