/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.executorservice;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.BlockingMessageTask;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;

import java.net.UnknownHostException;
import java.security.Permission;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.currentThread;

public abstract class AbstractExecutorServiceCancelMessageTask<P> extends AbstractCallableMessageTask<P>
        implements BlockingMessageTask {

    private static final int CANCEL_TRY_COUNT = 50;
    private static final int CANCEL_TRY_PAUSE_MILLIS = 250;

    public AbstractExecutorServiceCancelMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        InvocationBuilder builder = createInvocationBuilder();
        builder.setTryCount(CANCEL_TRY_COUNT).setTryPauseMillis(CANCEL_TRY_PAUSE_MILLIS);
        InternalCompletableFuture future = builder.invoke();
        boolean result = false;
        try {
            result = (Boolean) future.get();
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logException(e);
        } catch (ExecutionException e) {
            logException(e);
        }
        return result;
    }


    protected abstract InvocationBuilder createInvocationBuilder() throws UnknownHostException;

    private void logException(Exception e) {
        ILogger logger = nodeEngine.getLogger(AbstractExecutorServiceCancelMessageTask.class);
        logger.warning(e);
    }


    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }


    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "cancel";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}

