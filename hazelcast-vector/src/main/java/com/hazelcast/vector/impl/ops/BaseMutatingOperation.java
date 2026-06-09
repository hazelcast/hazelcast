/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.impl.storage.VectorCollectionStorage;

import static com.hazelcast.vector.impl.storage.VectorCollectionStorage.NOT_LOCKED;

/**
 * Common logic for all mutating operations which cannot be executed concurrently with optimization.
 * It is a base class both for primary and backup operations.
 * Main feature implemented in this class is waiting for the index to become unlocked before executing
 * and some common methods.
 */
public abstract class BaseMutatingOperation extends AbstractNamedOperation
        implements ServiceNamespaceAware, BlockingOperation {
    protected transient VectorCollectionStorage storage;

    // if the operation waited before
    protected transient boolean waited;

    // Note: this relies on order of invocation of shouldWait and getWaitKey, but avoids extra findLockedIndex invocations
    private transient Object lockedIndex;

    protected BaseMutatingOperation() {
    }

    protected BaseMutatingOperation(String vectorCollectionName) {
        super(vectorCollectionName);
    }

    @Override
    public void beforeRun() {
        VectorCollectionService service = getService();
        storage = service.getStorage(getName(), getPartitionId());
    }

    @Override
    public CallStatus call() throws Exception {
        if (waited) {
            getLogger().finest("Executing after waiting %s", this);
        }
        return super.call();
    }

    @Override
    public boolean shouldWait() {
        lockedIndex = storage.findLockedIndex();

        if (lockedIndex != NOT_LOCKED) {
            getLogger().finest("Index is locked, waiting with key %s, operation %s", getWaitKey(), this);
            waited = true;
        }
        return lockedIndex != NOT_LOCKED;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        assert lockedIndex != NOT_LOCKED : "getWaitKey invoked before shouldWait or without locked index";
        return new VectorMutationWaitNotifyKey(name, (String) lockedIndex, getPartitionId());
    }

    @Override
    public void onWaitExpire() {
        // this should not happen as at present vector collection does not use wait timeout
        // but return meaningful response just in case.
        sendResponse(new OperationTimeoutException("Timeout elapsed while waiting for index to be unlocked"));
    }

    @Override
    public String getServiceName() {
        return VectorCollectionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        VectorCollectionService service = getService();
        return service.getObjectNamespace(getName());
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (this instanceof BackupOperation) {
            ILogger logger = getLogger();
            if (waited) {
                // this is unparked execution which means that we have to sync replica to avoid inconsistency
                if (logger.isFineEnabled()) {
                    logger.fine(String.format("Backup operation failed, requesting replica sync: %s", this), e);
                }
                BackupUtil.markReplicaAsSyncRequiredForBackupOps(this);
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest(String.format("Backup operation failed %s", this), e);
                }
            }
        }
        super.onExecutionFailure(e);
    }
}
