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

import com.hazelcast.jet.impl.container.task.AbstractTask;
import com.hazelcast.jet.impl.data.io.NetworkTask;
import com.hazelcast.jet.impl.util.BooleanHolder;
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
        logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init() {
        closeSocket();

        totalBytes = 0;
        destroyed = false;
        finished = false;
        finalized = false;
        interrupted = false;
        waitingForFinish = false;
    }

    protected void stamp() {
        lastExecutionTimeOut = System.currentTimeMillis();
        inProgress = true;
    }

    protected void resetProgress() {
        inProgress = false;
    }

    @Override
    public void interrupt(Throwable error) {
        interrupted = true;
    }

    public void finalizeTask() {
        finalized = true;
    }

    @Override
    public void destroy() {
        try {
            closeSocket();
        } finally {
            finished = true;
            finalized = true;
            destroyed = true;
            inProgress = false;
            interrupted = true;
        }
    }

    protected boolean checkFinished() {
        if (finished) {
            notifyAMTaskFinished();
            return false;
        }

        return true;
    }

    protected void notifyAMTaskFinished() {

    }

    protected abstract boolean onExecute(BooleanHolder payload) throws Exception;

    @Override
    public boolean execute(BooleanHolder didWorkHolder) throws Exception {
        stamp();

        try {
            if (finalized) {
                waitingForFinish = true;
            }

            return onExecute(didWorkHolder);
        } finally {
            resetProgress();
        }
    }

    @Override
    public void closeSocket() {
        if (socketChannel != null) {
            try {
                socketChannel.close();
                socketChannel = null;
            } catch (IOException e) {
                logger.warning(e.getMessage(), e);
            }
        }
    }

    public boolean inProgress() {
        return inProgress;
    }

    @Override
    public long lastTimeStamp() {
        return lastExecutionTimeOut;
    }
}
