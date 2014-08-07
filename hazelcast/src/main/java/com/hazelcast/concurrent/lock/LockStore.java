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

import com.hazelcast.nio.serialization.Data;

import java.util.Set;

public interface LockStore {

    boolean lock(Data key, String caller, long threadId, long ttl);

    boolean txnLock(Data key, String caller, long threadId, long ttl);

    boolean extendLeaseTime(Data key, String caller, long threadId, long ttl);

    boolean unlock(Data key, String caller, long threadId);

    boolean isLocked(Data key);

    boolean isTransactionallyLocked(Data key);

    boolean isLockedBy(Data key, String caller, long threadId);

    int getLockCount(Data key);

    long getRemainingLeaseTime(Data key);

    boolean canAcquireLock(Data key, String caller, long threadId);

    Set<Data> getLockedKeys();

    boolean forceUnlock(Data dataKey);

    String getOwnerInfo(Data dataKey);
}
