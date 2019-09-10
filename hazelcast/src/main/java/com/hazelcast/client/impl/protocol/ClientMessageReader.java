/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Bits;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;

public class ClientMessageReader {

    private static final int INT_MASK = 0xffff;
    private int readIndex;
    private int readOffset = -1;
    private LinkedList<ClientMessage.Frame> frames = new LinkedList<>();

    public boolean readFrom(ByteBuffer src) {
        for (; ; ) {
            if (readFrame(src)) {
                if (ClientMessage.isFlagSet(frames.get(readIndex).flags, IS_FINAL_FLAG)) {
                    return true;
                }
                readIndex++;
                readOffset = -1;
            } else {
                return false;
            }

        }
    }

    public LinkedList<ClientMessage.Frame> getFrames() {
        return frames;
    }

    public void reset() {
        readIndex = 0;
        readOffset = -1;
        frames = new LinkedList<>();
    }

    private boolean readFrame(ByteBuffer src) {
        // init internal buffer
        int remaining = src.remaining();
        if (remaining < SIZE_OF_FRAME_LENGTH_AND_FLAGS) {
            // we don't have even the frame length and flags ready
            return false;
        }
        if (readOffset == -1) {
            int frameLength = Bits.readIntL(src, src.position());
            src.position(src.position() + Bits.INT_SIZE_IN_BYTES);
            int flags = Bits.readShortL(src, src.position()) & INT_MASK;
            src.position(src.position() + Bits.SHORT_SIZE_IN_BYTES);

            int size = frameLength - SIZE_OF_FRAME_LENGTH_AND_FLAGS;
            byte[] bytes = new byte[size];
            frames.add(new ClientMessage.Frame(bytes, flags));
            readOffset = 0;
            if (size == 0) {
                return true;
            }
        }

        ClientMessage.Frame frame = frames.get(readIndex);
        return accumulate(src, frame.content, frame.content.length - readOffset);
    }

    private boolean accumulate(ByteBuffer src, byte[] dest, int length) {
        int remaining = src.remaining();
        int readLength = remaining < length ? remaining : length;
        if (readLength > 0) {
            src.get(dest, readOffset, readLength);
            readOffset += readLength;
            return readLength == length;
        }
        return false;
    }

}
