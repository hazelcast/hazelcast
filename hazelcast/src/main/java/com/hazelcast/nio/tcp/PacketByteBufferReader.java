/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.nio.ByteBuffer;

/**
 * A {@link ByteBufferReader} that can read a {@link com.hazelcast.nio.Packet} using the
 * {@link com.hazelcast.nio.tcp.PacketReader}.
 */
public class PacketByteBufferReader implements ByteBufferReader {

    private final PacketReader packetReader;

    public PacketByteBufferReader(PacketReader packetReader) {
        this.packetReader = packetReader;
    }

    @Override
    public void read(ByteBuffer inBuffer) throws Exception {
        packetReader.readPacket(inBuffer);
    }
}
