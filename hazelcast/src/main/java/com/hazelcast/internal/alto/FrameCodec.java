/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.alto;


import com.hazelcast.internal.tpc.iobuffer.IOBuffer;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;

// always
// size: int
// flags: int
// partitionId: int  : 8
// request
// callid: long: 12
// opcode: 20
// response
// call id: long 12
public class FrameCodec {
    public static final int FLAG_OP = 1 << 1;
    public static final int FLAG_OP_RESPONSE = 1 << 2;
    public static final int FLAG_OP_RESPONSE_CONTROL = 1 << 3;
    public static final int RESPONSE_TYPE_OVERLOAD = 0;
    public static final int RESPONSE_TYPE_EXCEPTION = 1;
    public static final int OFFSET_SIZE = 0;
    public static final int OFFSET_FLAGS = OFFSET_SIZE + BYTES_INT;
    public static final int OFFSET_PARTITION_ID = OFFSET_FLAGS + BYTES_INT;
    public static final int OFFSET_RES_CALL_ID = OFFSET_PARTITION_ID + BYTES_INT;
    public static final int OFFSET_RES_PAYLOAD = OFFSET_RES_CALL_ID + BYTES_LONG;
    public static final int OFFSET_REQ_CALL_ID = OFFSET_PARTITION_ID + BYTES_INT;
    public static final int OFFSET_REQ_OPCODE = OFFSET_REQ_CALL_ID + BYTES_LONG;
    public static final int OFFSET_REQ_PAYLOAD = OFFSET_REQ_OPCODE + BYTES_INT;

    public static boolean isComplete(IOBuffer buff) {
        if (buff.position() < BYTES_INT) {
            // not enough bytes.
            return false;
        } else {
            return buff.position() == buff.getInt(0);
        }
    }

    public static int size(IOBuffer buff) {
        if (buff.byteBuffer().limit() < BYTES_INT) {
            return -1;
        }
        return buff.getInt(0);
    }

    public static void setSize(IOBuffer buffer, int size) {
        //ensure capacity?
        buffer.putInt(0, size);
    }

    public static void writeResponseHeader(IOBuffer buff, int partitionId, long callId) {
        writeResponseHeader(buff, partitionId, callId, 0);
    }

    public static void writeResponseHeader(IOBuffer buff, int partitionId, long callId, int flags) {
        buff.ensureRemaining(FrameCodec.OFFSET_RES_PAYLOAD);
        buff.writeInt(-1);  //size
        buff.writeInt(FrameCodec.FLAG_OP_RESPONSE | flags);
        buff.writeInt(partitionId);
        buff.writeLong(callId);
    }

    public static void writeRequestHeader(IOBuffer buff, int partitionId, int opcode) {
        buff.ensureRemaining(FrameCodec.OFFSET_REQ_PAYLOAD);
        buff.writeInt(-1); //size
        buff.writeInt(FrameCodec.FLAG_OP);
        buff.writeInt(partitionId);
        buff.writeLong(-1); //callid
        buff.writeInt(opcode);
    }

    // todo: the flip needs to be removed. we should just set the size.
    public static void setSize(IOBuffer buff) {
        ByteBuffer bb = buff.byteBuffer();
        buff.putInt(FrameCodec.OFFSET_SIZE, buff.position());
        buff.byteBuffer().flip();
    }

    public static void addFlags(IOBuffer buff, int addedFlags) {
        int oldFlags = buff.getInt(FrameCodec.OFFSET_FLAGS);
        buff.putInt(FrameCodec.OFFSET_FLAGS, oldFlags | addedFlags);
    }

    public static int flags(IOBuffer buff) {
        return buff.getInt(OFFSET_FLAGS);
    }

    public static boolean isFlagRaised(IOBuffer buff, int flag) {
        int flags = buff.getInt(OFFSET_FLAGS);
        return (flags & flag) != 0;
    }
}
