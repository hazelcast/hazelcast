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

package com.hazelcast.client.connection;

import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.serialization.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;

public class ProtocolWriter {
    protected static final byte[] HEADER = new byte[]{'P', '0', '1'};

    public ProtocolWriter(SerializationService serializationService) {
    }

    public void write(Connection connection, Protocol command) throws IOException {
        if (connection != null) {
            final ObjectDataOutputStream dos = connection.getOutputStream();
            if (!connection.headersWritten) {
                dos.write(HEADER);
                dos.write('\r');
                dos.write('\n');
                dos.flush();
                connection.headersWritten = true;
            }
            command.writeTo(dos);
        }
    }

    public void flush(Connection connection) throws IOException {
        if (connection != null) {
            ObjectDataOutputStream dos = connection.getOutputStream();
            dos.flush();
        }
    }
}
