/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class PacketReader extends PacketHandler {

    final ByteBuffer readHeaderBuffer = ByteBuffer.allocate(1 << 10); // 1k

    public Packet readPacket(Connection connection) throws IOException {
        final DataInputStream dis = connection.getInputStream();
        if (!connection.headerRead) {
            final byte[] b = new byte[3];
            dis.readFully(b);
            if (!Arrays.equals(HEADER, b)) {
                throw new IllegalStateException("Illegal header " + Arrays.toString(b));
            }
            connection.headerRead = true;
        }
        Packet response = new Packet();
        response.readFrom(this, dis);
        return response;
    }
}
