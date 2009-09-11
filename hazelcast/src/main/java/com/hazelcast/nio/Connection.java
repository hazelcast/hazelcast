/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.cluster.AddOrRemoveConnection;
import com.hazelcast.impl.CallContext;


import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

public class Connection {
    final SocketChannel socketChannel;

    final ReadHandler readHandler;

    final WriteHandler writeHandler;

    final ConnectionManager connectionManager;

    private volatile boolean live = true;

    Address endPoint = null;

    public Connection(ConnectionManager connectionManager, SocketChannel socketChannel) {
        this.connectionManager = connectionManager;
        this.socketChannel = socketChannel;
        this.writeHandler = new WriteHandler(this);
        this.readHandler = new ReadHandler(this);
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public ReadHandler getReadHandler() {
        return readHandler;
    }

    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    public void setLive(boolean live) {
        this.live = live;
    }

    public boolean live() {
        return live;
    }

    public Address getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(Address endPoint) {
        this.endPoint = endPoint;
    }

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((endPoint == null) ? 0 : endPoint.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final Connection other = (Connection) obj;
        if (endPoint == null) {
            if (other.endPoint != null)
                return false;
        } else if (!endPoint.equals(other.endPoint))
            return false;
        return true;
    }

    public void close() {
        if (!live)
            return;
        live = false;
        try {
            if (socketChannel != null && socketChannel.isOpen())
                socketChannel.close();
            readHandler.shutdown();
            writeHandler.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        connectionManager.remove(this);
        AddOrRemoveConnection addOrRemoveConnection = new AddOrRemoveConnection(endPoint, false);
        addOrRemoveConnection.setNode(connectionManager.node);
        connectionManager.node.clusterManager.enqueueAndReturn(addOrRemoveConnection);
    }

    @Override
    public String toString() {
        return "Connection [" + this.endPoint + "] live=" + live;
    }
}
