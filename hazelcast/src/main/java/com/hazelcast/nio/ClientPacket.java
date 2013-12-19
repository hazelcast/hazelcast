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

package com.hazelcast.nio;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationContext;

public final class ClientPacket extends DataAdapter implements SocketWritable, SocketReadable {

    private transient Connection conn;

    public ClientPacket(SerializationContext context) {
        super(context);
    }

    public ClientPacket(Data data) {
        super(data);
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(final Connection conn) {
        this.conn = conn;
    }

    public int size() {
        return data != null ? data.totalSize() : 0;
    }

    @Override
    public boolean isUrgent() {
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientPacket{");
        sb.append("conn=").append(conn);
        sb.append(", size=").append(size());
        sb.append('}');
        return sb.toString();
    }
}
