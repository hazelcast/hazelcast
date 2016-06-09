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

package com.hazelcast.concurrent.lock;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public final class LockStoreContainer {

    private final LockServiceImpl lockService;
    private final int partitionId;

    private final ConcurrentMap<ObjectNamespace, LockStoreImpl> lockStores =
            new ConcurrentHashMap<ObjectNamespace, LockStoreImpl>();
    private final AtomicReference<LockStoreImpl> internalLockStore = new AtomicReference<LockStoreImpl>();

    private final ConstructorFunction<ObjectNamespace, LockStoreImpl> lockStoreConstructor =
            new ConstructorFunction<ObjectNamespace, LockStoreImpl>() {
                public LockStoreImpl createNew(ObjectNamespace namespace) {
                    final ConstructorFunction<ObjectNamespace, LockStoreInfo> ctor =
                            lockService.getConstructor(namespace.getServiceName());
                    if (ctor != null) {
                        LockStoreInfo info = ctor.createNew(namespace);
                        if (info != null) {
                            int backupCount = info.getBackupCount();
                            int asyncBackupCount = info.getAsyncBackupCount();
                            EntryTaskScheduler entryTaskScheduler = createScheduler(namespace);
                            return new LockStoreImpl(lockService, namespace, entryTaskScheduler, backupCount, asyncBackupCount);
                        }
                    }
                    throw new IllegalArgumentException("No LockStore constructor is registered!");
                }
            };

    public LockStoreContainer(LockServiceImpl lockService, int partitionId) {
        this.lockService = lockService;
        this.partitionId = partitionId;
    }

    void clearLockStore(ObjectNamespace namespace) {
        if (isInternalNamespace(namespace)) {
            LockStoreImpl lockStore = internalLockStore.getAndSet(null);
            if (lockStore != null) {
                lockStore.clear();
            }
            return;
        }
        LockStoreImpl lockStore = lockStores.remove(namespace);
        if (lockStore != null) {
            lockStore.clear();
        }
    }

    LockStoreImpl getOrCreateLockStore(ObjectNamespace namespace) {
        if (isInternalNamespace(namespace)) {
            return ConcurrencyUtil.getOrSetIfAbsent(internalLockStore, lockStoreConstructor, namespace);
        }
        return ConcurrencyUtil.getOrPutIfAbsent(lockStores, namespace, lockStoreConstructor);
    }

    private boolean isInternalNamespace(ObjectNamespace namespace) {
        return namespace.getClass().equals(InternalLockNamespace.class);
    }

    LockStoreImpl getLockStore(ObjectNamespace namespace) {
        if (isInternalNamespace(namespace)) {
            return internalLockStore.get();
        }
        return lockStores.get(namespace);
    }

    public Collection<LockStoreImpl> getLockStores() {
        LockStoreImpl currentInternalLockStore = internalLockStore.get();
        if (currentInternalLockStore == null) {
            return Collections.unmodifiableCollection(lockStores.values());
        }
        ArrayList<LockStoreImpl> combinedStores = new ArrayList<LockStoreImpl>(lockStores.values());
        combinedStores.add(currentInternalLockStore);
        return combinedStores;
    }

    void clear() {
        for (LockStoreImpl lockStore : lockStores.values()) {
            lockStore.clear();
        }
        lockStores.clear();
        LockStoreImpl store = internalLockStore.getAndSet(null);
        if (store != null) {
            store.clear();
        }

    }

    int getPartitionId() {
        return partitionId;
    }

    public void put(LockStoreImpl lockStore) {
        lockStore.setLockService(lockService);
        ObjectNamespace namespace = lockStore.getNamespace();
        EntryTaskScheduler entryTaskScheduler = createScheduler(lockStore.getNamespace());
        lockStore.setEntryTaskScheduler(entryTaskScheduler);
        if (isInternalNamespace(namespace)) {
            internalLockStore.set(lockStore);
        } else {
            lockStores.put(lockStore.getNamespace(), lockStore);
        }

    }

    private EntryTaskScheduler createScheduler(ObjectNamespace namespace) {
        NodeEngine nodeEngine = lockService.getNodeEngine();
        LockEvictionProcessor entryProcessor = new LockEvictionProcessor(nodeEngine, namespace);
        TaskScheduler globalScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        return EntryTaskSchedulerFactory
                .newScheduler(globalScheduler, entryProcessor, ScheduleType.FOR_EACH);
    }
}
