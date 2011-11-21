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

package com.hazelcast.nio;

import com.hazelcast.cluster.AddOrRemoveConnection;
import com.hazelcast.config.AsymmetricEncryptionConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.Processable;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.logging.ILogger;

import java.util.logging.Level;

public class NodeIOService implements IOService {

    final Node node;

    public NodeIOService(Node node) {
        this.node = node;
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    public void onOutOfMemory(OutOfMemoryError oom) {
        node.onOutOfMemory(oom);
    }

    public void handleInterruptedException(Thread thread, RuntimeException e) {
        node.handleInterruptedException(thread, e);
    }

    public void onIOThreadStart() {
        ThreadContext.get().setCurrentFactory(node.factory);
    }

    public Address getThisAddress() {
        return node.getThisAddress();
    }

    public void onFatalError(Exception e) {
        node.shutdown(false, false);
    }

    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return node.getConfig().getNetworkConfig().getSymmetricEncryptionConfig();
    }

    public AsymmetricEncryptionConfig getAsymmetricEncryptionConfig() {
        return node.getConfig().getNetworkConfig().getAsymmetricEncryptionConfig();
    }

    public void handleClientPacket(Packet p) {
        node.clientService.handle(p);
    }

    public void handleMemberPacket(Packet p) {
        node.clusterService.enqueuePacket(p);
    }

    public TextCommandService getTextCommandService() {
        return node.getTextCommandService();
    }

    public boolean isMemcacheEnabled() {
        return node.groupProperties.MEMCACHE_ENABLED.getBoolean();
    }

    public boolean isRestEnabled() {
        return node.groupProperties.REST_ENABLED.getBoolean();
    }

    public void onConnectionClose(Address endPoint) {
        AddOrRemoveConnection addOrRemoveConnection = new AddOrRemoveConnection(endPoint, false);
        addOrRemoveConnection.setNode(node);
        node.clusterManager.enqueueAndReturn(addOrRemoveConnection);
    }

    public String getThreadPrefix() {
        return node.getName();
    }

    public void onFailedConnection(Address address) {
        if (!node.joined()) {
            node.failedConnection(address);
        }
    }

    public void shouldConnectTo(Address address) {
        if (node.getThisAddress().equals(address)) {
            throw new RuntimeException("Connecting to self! " + address);
        }
        if (!node.clusterManager.shouldConnectTo(address))
            throw new RuntimeException("Should not connect to " + address);
    }

    public int getSocketReceiveBufferSize() {
        return this.node.getGroupProperties().SOCKET_RECEIVE_BUFFER_SIZE.getInteger();
    }

    public int getSocketSendBufferSize() {
        return this.node.getGroupProperties().SOCKET_SEND_BUFFER_SIZE.getInteger();
    }

    public int getSocketLingerSeconds() {
        return this.node.getGroupProperties().SOCKET_LINGER_SECONDS.getInteger();
    }

    public boolean getSocketKeepAlive() {
        return this.node.getGroupProperties().SOCKET_KEEP_ALIVE.getBoolean();
    }

    public boolean getSocketNoDelay() {
        return this.node.getGroupProperties().SOCKET_NO_DELAY.getBoolean();
    }

    public int getSelectorThreadCount() {
        return node.groupProperties.IO_THREAD_COUNT.getInteger();
    }

    public boolean onDuplicateConnection(Address endPoint,
                                         boolean acceptTypeConnection,
                                         boolean accept,
                                         Connection connExisting) {
        final String msg = "Two connections from the same endpoint " + endPoint
                + ", acceptTypeConnection=" + acceptTypeConnection + ",  now accept="
                + accept;
        if (node.joined() && node.isMaster()) {
            node.getLogger(ConnectionManager.class.getName()).log(Level.WARNING, msg);
            connExisting.closeSilently();
            final Address deadEndpoint = connExisting.getEndPoint();
            if (deadEndpoint != null) {
                node.clusterManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        node.clusterManager.disconnectExistingCalls(deadEndpoint);
                    }
                });
            }
        } else {
            node.getLogger(ConnectionManager.class.getName()).log(Level.FINEST, msg);
            return true;
        }
        return false;
    }

    public boolean isClient() {
        return false;
    }
}

