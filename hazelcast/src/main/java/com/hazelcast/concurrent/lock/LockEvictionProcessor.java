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

import com.hazelcast.concurrent.lock.operations.UnlockIfLeaseExpiredOperation;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

import static com.hazelcast.concurrent.lock.LockServiceImpl.SERVICE_NAME;
import static com.hazelcast.spi.impl.ResponseHandlerFactory.createErrorLoggingResponseHandler;

public final class LockEvictionProcessor implements ScheduledEntryProcessor<Data, Object> {

    private final NodeEngine nodeEngine;
    private final ObjectNamespace namespace;

    public LockEvictionProcessor(NodeEngine nodeEngine, ObjectNamespace namespace) {
        this.nodeEngine = nodeEngine;
        this.namespace = namespace;
    }

    @Override
    public void process(EntryTaskScheduler<Data, Object> scheduler, Collection<ScheduledEntry<Data, Object>> entries) {
        for (ScheduledEntry<Data, Object> entry : entries) {
            Data key = entry.getKey();
            sendUnlockOperation(scheduler, key);
        }
    }

    private void sendUnlockOperation(EntryTaskScheduler<Data, Object> scheduler, Data key) {
        UnlockOperation operation = new UnlockIfLeaseExpiredOperation(namespace, key, scheduler);
        try {
            submit(operation, key);
        } catch (Throwable t) {
            ILogger logger = nodeEngine.getLogger(getClass());
            logger.warning(t);
        }
    }

    private void submit(UnlockOperation operation, Data key) {
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        operation.setNodeEngine(nodeEngine);
        operation.setServiceName(SERVICE_NAME);
        operation.setPartitionId(partitionId);
        OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
        operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
        operation.setResponseHandler(createErrorLoggingResponseHandler(nodeEngine.getLogger(operation.getClass())));
        operation.setAsyncBackup(true);

        operationService.executeOperation(operation);
    }

}
