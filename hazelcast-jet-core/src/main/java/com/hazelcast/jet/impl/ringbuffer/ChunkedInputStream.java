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

package com.hazelcast.jet.impl.ringbuffer;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.data.io.IOBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

class ChunkedInputStream extends InputStream {
    private static final int MASK = 0xff;

    private static final int BUFFER_OFFSET = HeapData.DATA_OFFSET;
    private final IOBuffer<JetPacket> packetStream;
    private byte[] buffer;
    private int bufferIdx;
    private Iterator<JetPacket> iterator;

    public ChunkedInputStream(IOBuffer<JetPacket> packetBuffer) {
        this.packetStream = packetBuffer;
    }

    @Override
    public int read() throws IOException {
        if (buffer == null) {
            if (iterator == null) {
                iterator = packetStream.iterator();
            }
            buffer = iterator.next().toByteArray();
        }
        try {
            if (bufferIdx == buffer.length - BUFFER_OFFSET - 1) {
                try {
                    return buffer[BUFFER_OFFSET + bufferIdx] & MASK;
                } finally {
                    buffer = null;
                    bufferIdx = 0;
                }
            } else {
                try {
                    return buffer[BUFFER_OFFSET + bufferIdx] & MASK;
                } finally {
                    bufferIdx++;
                }
            }
        } finally {
            if ((iterator != null) && (!iterator.hasNext())) {
                packetStream.reset();
                iterator = null;
            }
        }
    }

    public void onOpen() {
        if (buffer != null) {
            Arrays.fill(buffer, (byte) 0);
        }

        bufferIdx = 0;
    }
}
