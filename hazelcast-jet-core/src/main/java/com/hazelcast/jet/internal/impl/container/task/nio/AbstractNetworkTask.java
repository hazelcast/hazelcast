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

package com.hazelcast.jet.internal.impl.container.task.nio;

import com.hazelcast.jet.internal.api.data.io.NetworkTask;
import com.hazelcast.jet.internal.api.executor.Payload;
import com.hazelcast.jet.internal.impl.container.task.AbstractTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public abstract class AbstractNetworkTask extends AbstractTask
        implements NetworkTask {

    protected final ILogger logger;
    protected final Address jetAddress;
    protected boolean finished;
    protected long totalBytes;
    protected boolean waitingForFinish;
    protected volatile boolean destroyed;
    protected volatile boolean finalized;
    protected volatile boolean inProgress;
    protected volatile boolean interrupted;
    protected volatile SocketChannel socketChannel;
    protected volatile long lastExecutionTimeOut = -1;

    public AbstractNetworkTask(NodeEngine nodeEngine,
                               Address jetAddress) {
        this.jetAddress = jetAddress;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init() {
        closeSocket();

        this.totalBytes = 0;
        this.destroyed = false;
        this.finished = false;
        this.finalized = false;
        this.interrupted = false;
        this.waitingForFinish = false;
    }

    protected void stamp() {
        this.lastExecutionTimeOut = System.currentTimeMillis();
        this.inProgress = true;
    }

    protected void resetProgress() {
        this.inProgress = false;
    }

    @Override
    public void interrupt(Throwable error) {
        this.interrupted = true;
    }

    public void finalizeTask() {
        this.finalized = true;
    }

    @Override
    public void destroy() {
        try {
            closeSocket();
        } finally {
            this.finished = true;
            this.finalized = true;
            this.destroyed = true;
            this.inProgress = false;
            this.interrupted = true;
        }
    }

    protected boolean checkFinished() {
        if (this.finished) {
            notifyAMTaskFinished();
            return false;
        }

        return true;
    }

    protected void notifyAMTaskFinished() {

    }

    protected abstract boolean onExecute(Payload payload) throws Exception;

    @Override
    public boolean executeTask(Payload payload) throws Exception {
        stamp();

        try {
            if (this.finalized) {
                this.waitingForFinish = true;
            }

            return onExecute(payload);
        } finally {
            resetProgress();
        }
    }

    @Override
    public void closeSocket() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
                this.socketChannel = null;
            } catch (IOException e) {
                this.logger.warning(e.getMessage(), e);
            }
        }
    }

    public boolean inProgress() {
        return this.inProgress;
    }

    @Override
    public long lastTimeStamp() {
        return this.lastExecutionTimeOut;
    }
}
