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

package com.hazelcast.client.impl.protocol;

import com.hazelcast.internal.nio.Bits;

import java.nio.ByteBuffer;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;
import static com.hazelcast.internal.util.JVMUtil.upcast;

public class ClientMessageWriter {

    private transient ClientMessage.Frame currentFrame;
    //-1 means length is not written yet
    private transient int writeOffset = -1;

    public boolean writeTo(ByteBuffer dst, ClientMessage clientMessage) {
        if (currentFrame == null) {
            currentFrame = clientMessage.startFrame;
        }
        for (; ; ) {
            boolean isLastFrame = currentFrame.next == null;
            if (writeFrame(dst, currentFrame, isLastFrame)) {
                writeOffset = -1;
                if (isLastFrame) {
                    currentFrame = null;
                    return true;
                }
                currentFrame = currentFrame.next;
            } else {
                return false;
            }
        }
    }

    private boolean writeFrame(ByteBuffer dst, ClientMessage.Frame frame, boolean isLastFrame) {
        // the number of bytes that can be written to the bb
        int bytesWritable = dst.remaining();
        int frameContentLength = frame.content.length;

        //if write offset is -1 put the length and flags byte first
        if (writeOffset == -1) {
            if (bytesWritable >= SIZE_OF_FRAME_LENGTH_AND_FLAGS) {
                Bits.writeIntL(dst, dst.position(), frameContentLength + SIZE_OF_FRAME_LENGTH_AND_FLAGS);
                upcast(dst).position(dst.position() + Bits.INT_SIZE_IN_BYTES);

                if (isLastFrame) {
                    Bits.writeShortL(dst, dst.position(), (short) (frame.flags | IS_FINAL_FLAG));
                } else {
                    Bits.writeShortL(dst, dst.position(), (short) frame.flags);
                }
                upcast(dst).position(dst.position() + Bits.SHORT_SIZE_IN_BYTES);
                writeOffset = 0;
            } else {
                return false;
            }
        }
        bytesWritable = dst.remaining();

        if (frameContentLength == 0) {
            return true;
        }

        // the number of bytes that need to be written
        int bytesNeeded = frameContentLength - writeOffset;

        int bytesWrite;
        boolean done;
        if (bytesWritable >= bytesNeeded) {
            // all bytes for the value are available
            bytesWrite = bytesNeeded;
            done = true;
        } else {
            // not all bytes for the value are available. Write as much as is available
            bytesWrite = bytesWritable;
            done = false;
        }

        dst.put(frame.content, writeOffset, bytesWrite);
        writeOffset += bytesWrite;

        return done;
    }
}
