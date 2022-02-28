/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.util.Iterator;
import java.util.logging.Level;

public class CacheClearExpiredOperation extends AbstractLocalOperation
        implements PartitionAwareOperation, MutatingOperation {

    private int expirationPercentage;

    public CacheClearExpiredOperation(int expirationPercentage) {
        this.expirationPercentage = expirationPercentage;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        if (getNodeEngine().getLocalMember().isLiteMember()) {
            // this operation shouldn't run on lite members. This situation can potentially be seen
            // when converting a data-member to lite-member during merge operations.
            return;
        }

        if (!isOwner()) {
            return;
        }

        CacheService service = getService();
        CachePartitionSegment segment = service.getSegment(getPartitionId());
        Iterator<ICacheRecordStore> iterator = segment.recordStoreIterator();
        while (iterator.hasNext()) {
            ICacheRecordStore store = iterator.next();
            if (store.size() > 0) {
                store.evictExpiredEntries(expirationPercentage);
            }
        }
    }

    private boolean isOwner() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Address owner = nodeEngine.getPartitionService().getPartitionOwner(getPartitionId());
        return nodeEngine.getThisAddress().equals(owner);
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        try {
            super.onExecutionFailure(e);
        } finally {
            prepareForNextCleanup();
        }
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof PartitionMigratingException) {
            ILogger logger = getLogger();
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, e.toString());
            }
        } else {
            super.logError(e);
        }
    }

    @Override
    public void afterRun() throws Exception {
        prepareForNextCleanup();
    }

    protected void prepareForNextCleanup() {
        CacheService service = getService();
        CachePartitionSegment segment = service.getSegment(getPartitionId());
        segment.setRunningCleanupOperation(false);
        segment.setLastCleanupTime(Clock.currentTimeMillis());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", expirationPercentage=").append(expirationPercentage);
    }
}
