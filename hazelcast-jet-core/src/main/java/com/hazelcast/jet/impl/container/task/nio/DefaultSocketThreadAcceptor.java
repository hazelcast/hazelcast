/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.container.task.nio;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.data.io.NetworkTask;
import com.hazelcast.jet.impl.data.io.SocketReader;
import com.hazelcast.jet.impl.JetApplicationManager;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.impl.executor.Payload;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;

public class DefaultSocketThreadAcceptor extends DefaultSocketReader {
    private final ServerSocketChannel serverSocketChannel;
    private final JetApplicationManager jetApplicationManager;

    private long lastConnectionsTimeChecking = -1;

    public DefaultSocketThreadAcceptor(
            JetApplicationManager jetApplicationManager,
            NodeEngine nodeEngine,
            ServerSocketChannel serverSocketChannel
    ) {
        super(nodeEngine);
        this.serverSocketChannel = serverSocketChannel;
        this.jetApplicationManager = jetApplicationManager;
    }

    protected boolean consumePacket(JetPacket packet) throws Exception {
        if (packet.getHeader() == JetPacket.HEADER_JET_MEMBER_EVENT) {
            NodeEngine nodeEngine = this.jetApplicationManager.getNodeEngine();

            String applicationName = nodeEngine.getSerializationService().toObject(
                    new HeapData(packet.getApplicationNameBytes())
            );

            ApplicationContext applicationContext = this.jetApplicationManager.getApplicationContext(applicationName);

            if (applicationContext != null) {
                Address address = applicationContext.getNodeEngine().getSerializationService().toObject(
                        new HeapData(packet.toByteArray())
                );

                SocketReader reader = applicationContext.getSocketReaders().get(address);
                reader.setSocketChannel(this.socketChannel, this.receiveBuffer, true);
                this.socketChannel = null;
                this.receiveBuffer = null;
            } else {
                alignBuffer(this.receiveBuffer);
            }
        }

        return false;
    }

    @Override
    public boolean executeTask(Payload payload) throws Exception {
        checkConnectivity();

        if (this.socketChannel != null) {
            if (this.receiveBuffer == null) {
                this.receiveBuffer = ByteBuffer.allocateDirect(
                        ApplicationConfig.DEFAULT_TCP_BUFFER_SIZE
                ).order(ByteOrder.BIG_ENDIAN);
            }

            return super.executeTask(payload);
        }

        try {
            SocketChannel socketChannel = this.serverSocketChannel.accept();

            if (socketChannel != null) {
                payload.set(true);
                this.socketChannel = socketChannel;
            } else {
                payload.set(false);
            }
        } catch (IOException e) {
            return true;
        }

        return true;
    }

    private void checkConnectivity() {
        if ((this.lastConnectionsTimeChecking > 0)
                &&
                (System.currentTimeMillis() - this.lastConnectionsTimeChecking
                        >=
                        ApplicationConfig.DEFAULT_CONNECTIONS_CHECKING_INTERVAL_MS)
                ) {
            checkSocketChannels();
            this.lastConnectionsTimeChecking = System.currentTimeMillis();
        }
    }

    private void checkSocketChannels() {
        for (ApplicationContext applicationContext : this.jetApplicationManager.getApplicationContexts()) {
            checkTasksActivity(applicationContext.getSocketReaders().values());
            checkTasksActivity(applicationContext.getSocketWriters().values());
        }
    }

    private void checkTasksActivity(Collection<? extends NetworkTask> networkTasks) {
        for (NetworkTask networkTask : networkTasks) {
            if (networkTask.inProgress()) {
                if ((networkTask.lastTimeStamp() > 0)
                        &&
                        (System.currentTimeMillis() - networkTask.lastTimeStamp()
                                >
                                ApplicationConfig.DEFAULT_CONNECTIONS_SILENCE_TIMEOUT_MS)) {
                    networkTask.closeSocket();
                }
            }
        }
    }

    @Override
    public void finalizeTask() {
        try {
            this.serverSocketChannel.close();
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }
}
