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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;

/**
 * A {@link WriteHandler} that for member to member communication.
 *
 * It writes {@link Packet} instances to the {@link ByteBuffer}.
 *
 * @see MemberReadHandler
 */
public class MemberWriteHandler implements WriteHandler<Packet> {
    private boolean headerComplete;
    private int valueOffset;
    private int size;

    @Override
    public boolean onWrite(Packet packet, ByteBuffer dst) {
        byte[] payload = packet.toByteArray();
        if (!headerComplete) {
            if (dst.remaining() < Packet.HEADER_SIZE) {
                return false;
            }

            dst.put(Packet.VERSION);
            dst.putShort(packet.getFlags());
            dst.putInt(packet.getPartitionId());
            size = payload == null ? 0 : payload.length;
            dst.putInt(size);
            headerComplete = true;
        }

        if (size > 0) {
            // the number of bytes that can be written to the bb.
            int bytesWritable = dst.remaining();

            // the number of bytes that need to be written.
            int bytesNeeded = size - valueOffset;

            // the number of bytes that are going to be written
            int bytesWriting;
            boolean done;
            if (bytesWritable >= bytesNeeded) {
                // All bytes for the value are available.
                bytesWriting = bytesNeeded;
                done = true;
            } else {
                // Not all bytes for the value are available. So lets write as much as is available.
                bytesWriting = bytesWritable;
                done = false;
            }

            dst.put(payload, valueOffset, bytesWriting);
            valueOffset += bytesWriting;

            if (!done) {
                return false;
            }
        }

        headerComplete = false;
        valueOffset = 0;
        size = 0;
        return true;
    }
}
