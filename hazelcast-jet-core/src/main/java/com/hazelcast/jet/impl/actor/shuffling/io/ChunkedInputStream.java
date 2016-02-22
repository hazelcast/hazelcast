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

package com.hazelcast.jet.impl.actor.shuffling.io;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.impl.hazelcast.JetPacket;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

public class ChunkedInputStream extends InputStream {
    private static final int MASK = 0xff;

    private static final int BUFFER_OFFSET = HeapData.DATA_OFFSET;
    private final DefaultObjectIOStream<JetPacket> packetStream;
    private byte[] buffer;
    private int bufferIdx;
    private Iterator<JetPacket> iterator;

    public ChunkedInputStream(DefaultObjectIOStream<JetPacket> packetStream) {
        this.packetStream = packetStream;
    }

    @Override
    public int read() throws IOException {
        if (this.buffer == null) {
            if (this.iterator == null) {
                this.iterator = this.packetStream.iterator();
            }

            this.buffer = this.iterator.next().toByteArray();
        }

        try {
            if (this.bufferIdx == this.buffer.length - BUFFER_OFFSET - 1) {
                try {
                    return this.buffer[BUFFER_OFFSET + this.bufferIdx] & MASK;
                } finally {
                    this.buffer = null;
                    this.bufferIdx = 0;
                }
            } else {
                try {
                    return this.buffer[BUFFER_OFFSET + this.bufferIdx] & MASK;
                } finally {
                    this.bufferIdx++;
                }
            }
        } finally {
            if ((this.iterator != null) && (!this.iterator.hasNext())) {
                this.packetStream.reset();
                this.iterator = null;
            }
        }
    }

    public void onOpen() {
        if (this.buffer != null) {
            Arrays.fill(this.buffer, (byte) 0);
        }

        this.bufferIdx = 0;
    }
}
