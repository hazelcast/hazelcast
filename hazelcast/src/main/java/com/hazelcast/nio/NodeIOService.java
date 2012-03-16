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

import com.hazelcast.cluster.AddOrRemoveConnection;
import com.hazelcast.config.AsymmetricEncryptionConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.Processable;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.impl.base.SystemLogService;
import com.hazelcast.logging.ILogger;

public class NodeIOService implements IOService {

    final Node node;

    public NodeIOService(Node node) {
        this.node = node;
    }

    public boolean isActive() {
        return node.isActive();
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    public SystemLogService getSystemLogService() {
        return node.getSystemLogService();
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
        getSystemLogService().logConnection(e.getClass().getName() + ": " + e.getMessage());
        node.shutdown(false, false);
    }

    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return node.getConfig().getNetworkConfig().getSocketInterceptorConfig();
    }

    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return node.getConfig().getNetworkConfig().getSymmetricEncryptionConfig();
    }

    public AsymmetricEncryptionConfig getAsymmetricEncryptionConfig() {
        return node.getConfig().getNetworkConfig().getAsymmetricEncryptionConfig();
    }

    public SSLConfig getSSLConfig() {
        return node.getConfig().getNetworkConfig().getSSLConfig();
    }

    public void handleClientPacket(Packet p) {
        node.clientHandlerService.handle(p);
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

    public void removeEndpoint(Address endPoint) {
        AddOrRemoveConnection addOrRemoveConnection = new AddOrRemoveConnection(endPoint, false);
        addOrRemoveConnection.setNode(node);
        node.clusterManager.enqueueAndReturn(addOrRemoveConnection);
    }

    public String getThreadPrefix() {
        return node.getThreadPoolNamePrefix("IO");
    }

    public ThreadGroup getThreadGroup() {
        return node.threadGroup;
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
    }

    public boolean isReuseSocketAddress() {
        return node.getConfig().isReuseAddress();
    }

    public int getSocketPort() {
        return node.getConfig().getPort();
    }

    public boolean isSocketBindAny() {
        return node.groupProperties.SOCKET_BIND_ANY.getBoolean();
    }

    public boolean isSocketPortAutoIncrement() {
        return node.getConfig().isPortAutoIncrement();
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

    public void disconnectExistingCalls(final Address deadEndpoint) {
        if (deadEndpoint != null) {
            node.clusterManager.enqueueAndReturn(new Processable() {
                public void process() {
                    node.clusterManager.disconnectExistingCalls(deadEndpoint);
                }
            });
        }
    }

    public boolean isClient() {
        return false;
    }

    public long getConnectionMonitorInterval() {
        return node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong();
    }

    public int getConnectionMonitorMaxFaults() {
        return node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS.getInteger();
    }

    public void onShutdown() {
        node.clusterManager.sendProcessableToAll(new AddOrRemoveConnection(getThisAddress(), false), false);
        try {
            // wait a little to
            Thread.sleep(100 * node.getClusterImpl().getMembers().size());
        } catch (InterruptedException e) {
        }
    }
}

