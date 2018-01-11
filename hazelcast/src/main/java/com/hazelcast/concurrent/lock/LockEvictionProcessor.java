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

package com.hazelcast.concurrent.lock;

import com.hazelcast.concurrent.lock.operations.UnlockIfLeaseExpiredOperation;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

import static com.hazelcast.concurrent.lock.LockServiceImpl.SERVICE_NAME;

public final class LockEvictionProcessor implements ScheduledEntryProcessor<Data, Integer> {

    private final NodeEngine nodeEngine;
    private final ObjectNamespace namespace;
    private final ILogger logger;
    private final OperationResponseHandler unlockResponseHandler;

    public LockEvictionProcessor(NodeEngine nodeEngine, ObjectNamespace namespace) {
        this.nodeEngine = nodeEngine;
        this.namespace = namespace;
        this.logger = nodeEngine.getLogger(getClass());
        this.unlockResponseHandler = new UnlockResponseHandler();
    }

    @Override
    public void process(EntryTaskScheduler<Data, Integer> scheduler, Collection<ScheduledEntry<Data, Integer>> entries) {
        for (ScheduledEntry<Data, Integer> entry : entries) {
            Data key = entry.getKey();
            int version = entry.getValue();
            sendUnlockOperation(key, version);
        }
    }

    private void sendUnlockOperation(Data key, int version) {
        UnlockOperation operation = new UnlockIfLeaseExpiredOperation(namespace, key, version);
        try {
            submit(operation, key);
        } catch (Throwable t) {
            logger.warning(t);
        }
    }

    private void submit(UnlockOperation operation, Data key) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        OperationService operationService = nodeEngine.getOperationService();
        operation.setPartitionId(partitionId);
        operation.setOperationResponseHandler(unlockResponseHandler);
        operation.setValidateTarget(false);
        operation.setAsyncBackup(true);

        operationService.invokeOnTarget(SERVICE_NAME, operation, nodeEngine.getThisAddress());
    }

    private class UnlockResponseHandler implements OperationResponseHandler {
        @Override
        public void sendResponse(Operation op, Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                if (t instanceof RetryableException) {
                    logger.finest("While unlocking... " + t.getMessage());
                } else {
                    logger.warning(t);
                }
            }
        }
    }
}
