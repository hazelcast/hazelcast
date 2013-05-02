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

package com.hazelcast.cluster;

import com.hazelcast.nio.*;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

public class BindOperation extends AbstractClusterOperation implements JoinOperation {

    private Address localAddress;
    private Address targetAddress;
    private boolean replyBack = false;

    public BindOperation() {
    }

    public BindOperation(Address localAddress) {
        this(localAddress, null, false);
    }

    public BindOperation(Address localAddress, final Address targetAddress, final boolean replyBack) {
        this.localAddress = localAddress;
        this.targetAddress = targetAddress;
        this.replyBack = replyBack;
    }

    public void run() {
        NodeEngineImpl ns = (NodeEngineImpl) getNodeEngine();
        TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) ns.getNode().getConnectionManager();
        TcpIpConnection connection = (TcpIpConnection) getConnection();
        connectionManager.bind(connection, localAddress, targetAddress, replyBack);
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        localAddress = new Address();
        localAddress.readData(in);
        boolean hasTarget = in.readBoolean();
        if (hasTarget) {
            targetAddress = new Address();
            targetAddress.readData(in);
        }
        replyBack = in.readBoolean();
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        localAddress.writeData(out);
        boolean hasTarget = targetAddress != null;
        out.writeBoolean(hasTarget);
        if (hasTarget) {
            targetAddress.writeData(out);
        }
        out.writeBoolean(replyBack);
    }

    @Override
    public String toString() {
        return "Bind " + localAddress;
    }
}
