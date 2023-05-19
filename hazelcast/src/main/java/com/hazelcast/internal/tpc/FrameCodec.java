/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc;


import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;

// always
// size: int
// flags: int
// partitionId: int  : 8
// request
// callid: long: 12
// commandid: 20
// response
// call id: long 12
public final class FrameCodec {
    // It is a request.
    public static final int FLAG_REQ = 1 << 1;
    // it is a normal response.
    public static final int FLAG_RES = 1 << 2;
    // it is a special response (e.g. exception)
    public static final int FLAG_RES_CTRL = 1 << 3;
    // Classic Hazelcast payload.
    public static final int FLAG_PACKET = 1 << 4;
    // If the frame should be processed with priority
    public static final int FLAG_PRIORITY = 1 << 5;

    // todo: redirect should be added here.
    public static final int RES_CTRL_TYPE_OVERLOAD = 0;
    public static final int RES_CTRL_TYPE_EXCEPTION = 1;

    public static final int OFFSET_SIZE = 0;
    public static final int OFFSET_FLAGS = OFFSET_SIZE + SIZEOF_INT;
    public static final int OFFSET_PARTITION_ID = OFFSET_FLAGS + SIZEOF_INT;

    public static final int OFFSET_RES_CALL_ID = OFFSET_PARTITION_ID + SIZEOF_INT;
    public static final int OFFSET_RES_PAYLOAD = OFFSET_RES_CALL_ID + SIZEOF_LONG;

    public static final int OFFSET_REQ_CALL_ID = OFFSET_PARTITION_ID + SIZEOF_INT;
    public static final int OFFSET_REQ_CMD_ID = OFFSET_REQ_CALL_ID + SIZEOF_LONG;
    public static final int OFFSET_REQ_PAYLOAD = OFFSET_REQ_CMD_ID + SIZEOF_INT;

    private FrameCodec() {
    }

    public static boolean isComplete(IOBuffer frame) {
        if (frame.position() < SIZEOF_INT) {
            // not enough bytes.
            return false;
        } else {
            return frame.position() == frame.getInt(0);
        }
    }

    public static int size(IOBuffer frame) {
        if (frame.byteBuffer().limit() < SIZEOF_INT) {
            return -1;
        } else {
            return frame.getInt(0);
        }
    }

    public static void setSize(IOBuffer frame, int size) {
        //ensure capacity?
        frame.putInt(0, size);
    }

    public static void writeResponseHeader(IOBuffer frame, int partitionId, long callId) {
        writeResponseHeader(frame, partitionId, callId, 0);
    }

    public static void writeResponseHeader(IOBuffer frame, int partitionId, long callId, int flags) {
        frame.ensureRemaining(OFFSET_RES_PAYLOAD);
        //size
        frame.writeInt(-1);
        frame.writeInt(FLAG_RES | flags);

        frame.writeInt(partitionId);
        frame.writeLong(callId);
    }

    public static void writeRequestHeader(IOBuffer frame, int partitionId, int commandId) {
        frame.ensureRemaining(OFFSET_REQ_PAYLOAD);
        //size
        frame.writeInt(-1);
        frame.writeInt(FrameCodec.FLAG_REQ);
        frame.writeInt(partitionId);
        //callid
        frame.writeLong(-1);
        frame.writeInt(commandId);
    }

    // todo: the flip needs to be removed. we should just set the size.
    public static void setSize(IOBuffer frame) {
        frame.putInt(OFFSET_SIZE, frame.position());
        frame.flip();
    }

    public static void addFlags(IOBuffer frame, int addedFlags) {
        int oldFlags = frame.getInt(OFFSET_FLAGS);
        frame.putInt(OFFSET_FLAGS, oldFlags | addedFlags);
    }

    public static int flags(IOBuffer frame) {
        return frame.getInt(OFFSET_FLAGS);
    }

    public static boolean isFlagRaised(IOBuffer frame, int flag) {
        int flags = frame.getInt(OFFSET_FLAGS);
        return (flags & flag) != 0;
    }
}
