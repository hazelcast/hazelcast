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

package com.hazelcast.concurrent.lock;

import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.concurrent.lock.LockServiceImpl.SERVICE_NAME;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

public final class LockEvictionProcessor implements ScheduledEntryProcessor<Data, Object> {

    private static final int AWAIT_COMPLETION_TIMEOUT_SECONDS = 30;

    private final FutureUtil.ExceptionHandler exceptionHandler;
    private final NodeEngine nodeEngine;
    private final ObjectNamespace namespace;

    public LockEvictionProcessor(NodeEngine nodeEngine, ObjectNamespace namespace) {
        this.nodeEngine = nodeEngine;
        this.namespace = namespace;
        this.exceptionHandler = new AwaitCompletionExceptionHandler(nodeEngine, getClass());
    }

    @Override
    public void process(EntryTaskScheduler<Data, Object> scheduler, Collection<ScheduledEntry<Data, Object>> entries) {
        Collection<Future> futures = sendUnlockOperations(entries);
        waitWithDeadline(futures, AWAIT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS, exceptionHandler);
    }

    private Collection<Future> sendUnlockOperations(Collection<ScheduledEntry<Data, Object>> entries) {
        Collection<Future> futures = new ArrayList<Future>(entries.size());

        for (ScheduledEntry<Data, Object> entry : entries) {
            Data key = entry.getKey();
            sendUnlockOperation(futures, key);
        }
        return futures;
    }

    private void sendUnlockOperation(Collection<Future> futures, Data key) {
        Operation operation = new UnlockOperation(namespace, key, -1, true);
        try {
            Future f = invoke(operation, key);
            futures.add(f);
        } catch (Throwable t) {
            ILogger logger = nodeEngine.getLogger(getClass());
            logger.warning(t);
        }
    }

    private InternalCompletableFuture invoke(Operation operation, Data key) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
    }

    private static final class AwaitCompletionExceptionHandler implements FutureUtil.ExceptionHandler {
        private final ILogger logger;

        private AwaitCompletionExceptionHandler(NodeEngine nodeEngine, Class<?> type) {
            this.logger = nodeEngine.getLogger(type);
        }

        @Override
        public void handleException(Throwable throwable) {
            if (throwable instanceof TimeoutException) {
                logger.finest(throwable);
            } else if (throwable instanceof Exception) {
                logger.warning(throwable);
            }
        }
    }
}
