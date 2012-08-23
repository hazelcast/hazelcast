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
    //    private final String name;
    private final Data value;
    private final long callId;
    private final int partitionId;
    private final int replicaIndex;
    private final boolean targetAware;
    private final Connection conn;

    public SimpleSocketWritable(ClusterOperation op, String name, Data value, long callId, int partitionId, int replicaIndex, Connection conn, boolean targetAware) {
        this.op = op;
//        this.name = name;
        this.value = value;
        this.callId = callId;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.targetAware = targetAware;
        this.conn = conn;
    }

    public SimpleSocketWritable(Packet packet) {
        op = packet.operation;
//        name = packet.name;
        value = packet.getValueData();
        conn = packet.conn;
        partitionId = packet.blockId;
        callId = packet.callId;
        replicaIndex = packet.threadId;
        targetAware = packet.longValue == 1;
    }

    public ClusterOperation getOp() {
        return op;
    }

    public String getName() {
        return null; //name;
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

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public Connection getConn() {
        return conn;
    }

    public boolean isTargetAware() {
        return targetAware;
    }

    public void onEnqueue() {
    }

    void setToPacket(Packet packet) {
        packet.operation = op;
//        packet.name = name;
        packet.setValue(value);
        packet.callId = callId;
        packet.blockId = partitionId;
        packet.threadId = replicaIndex;
        packet.longValue = (targetAware) ? 1 : 0;
    }
}
