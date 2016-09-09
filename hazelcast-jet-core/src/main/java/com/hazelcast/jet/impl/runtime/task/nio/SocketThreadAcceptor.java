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

package com.hazelcast.jet.impl.runtime.task.nio;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.job.JobService;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class SocketThreadAcceptor extends SocketReader {
    private final ServerSocketChannel serverSocketChannel;
    private final JobService jobService;

    private long lastConnectionsTimeChecking = -1;

    public SocketThreadAcceptor(
            JobService jobService,
            NodeEngine nodeEngine,
            ServerSocketChannel serverSocketChannel
    ) {
        super(nodeEngine);
        this.serverSocketChannel = serverSocketChannel;
        this.jobService = jobService;
    }

    protected boolean consumePacket(JetPacket packet) {
        if (packet.getHeader() == JetPacket.HEADER_JET_MEMBER_EVENT) {
            NodeEngine nodeEngine = this.jobService.getNodeEngine();

            String jobName = nodeEngine.getSerializationService().toObject(
                    new HeapData(packet.getJobNameBytes())
            );

            JobContext jobContext = this.jobService.getContext(jobName);

            if (jobContext != null) {
                Address address = jobContext.getNodeEngine().getSerializationService().toObject(
                        new HeapData(packet.toByteArray())
                );

                SocketReader reader = jobContext.getSocketReaders().get(address);
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
    public boolean execute(BooleanHolder didWorkHolder) throws Exception {
        checkConnectivity();

        if (this.socketChannel != null) {
            if (this.receiveBuffer == null) {
                this.receiveBuffer = ByteBuffer.allocateDirect(
                        JobConfig.DEFAULT_TCP_BUFFER_SIZE
                ).order(ByteOrder.BIG_ENDIAN);
            }

            return super.execute(didWorkHolder);
        }

        try {
            SocketChannel socketChannel = this.serverSocketChannel.accept();

            if (socketChannel != null) {
                didWorkHolder.set(true);
                this.socketChannel = socketChannel;
            } else {
                didWorkHolder.set(false);
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
                        JobConfig.DEFAULT_CONNECTIONS_CHECKING_INTERVAL_MS)
                ) {
            checkSocketChannels();
            this.lastConnectionsTimeChecking = System.currentTimeMillis();
        }
    }

    private void checkSocketChannels() {
        for (JobContext jobContext : this.jobService.getJobContextMap()) {
            checkTasksActivity(jobContext.getSocketReaders().values());
            checkTasksActivity(jobContext.getSocketWriters().values());
        }
    }

    private void checkTasksActivity(Collection<? extends NetworkTask> networkTasks) {
        for (NetworkTask networkTask : networkTasks) {
            if (networkTask.inProgress()) {
                if ((networkTask.lastTimeStamp() > 0)
                        &&
                        (System.currentTimeMillis() - networkTask.lastTimeStamp()
                                >
                                JobConfig.DEFAULT_CONNECTIONS_SILENCE_TIMEOUT_MS)) {
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
            throw unchecked(e);
        }
    }
}
