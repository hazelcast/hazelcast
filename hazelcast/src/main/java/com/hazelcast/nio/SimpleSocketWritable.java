/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.impl.ClusterOperation;

public class SimpleSocketWritable implements SocketWritable {
    private final ClusterOperation op;
    private final String name;
    private final Data value;
    private final long callId;
    private final int partitionId;
    private final Connection conn;
    private final boolean nonBlocking;

    public SimpleSocketWritable(ClusterOperation op, String name, Data value, long callId, int partitionId, Connection conn, boolean nonBlocking) {
        this.op = op;
        this.name = name;
        this.value = value;
        this.callId = callId;
        this.partitionId = partitionId;
        this.conn = conn;
        this.nonBlocking = nonBlocking;
    }

    public SimpleSocketWritable(Packet packet) {
        op = packet.operation;
        name = packet.name;
        value = packet.getValueData();
        conn = packet.conn;
        callId = packet.callId;
        partitionId = packet.blockId;
        nonBlocking = packet.longValue == 1;
    }

    public ClusterOperation getOp() {
        return op;
    }

    public String getName() {
        return name;
    }

    public Data getValue() {
        return value;
    }

    public long getCallId() {
        return callId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Connection getConn() {
        return conn;
    }

    public boolean isNonBlocking() {
        return nonBlocking;
    }

    public void onEnqueue() {
    }

    void setToPacket(Packet packet) {
        packet.operation = op;
        packet.name = name;
        packet.setValue(value);
        packet.callId = callId;
        packet.blockId = partitionId;
        packet.longValue = (nonBlocking) ? 1 : 0;
    }
}
