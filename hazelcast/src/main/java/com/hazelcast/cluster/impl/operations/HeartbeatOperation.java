/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl.operations;

import com.hazelcast.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public final class HeartbeatOperation extends AbstractClusterOperation
        implements JoinOperation, IdentifiedDataSerializable {

    private long timestamp;

    public HeartbeatOperation() {
    }

    public HeartbeatOperation(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        MemberImpl member = service.getMember(getCallerAddress());
        if (member == null) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Heartbeat received from an unknown endpoint: " + getCallerAddress());
            }
            return;
        }
        service.getClusterHeartbeatManager().onHeartbeat(member, timestamp);
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.HEARTBEAT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(timestamp);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        timestamp = in.readLong();
    }
}
