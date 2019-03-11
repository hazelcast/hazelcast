/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Map.Entry;
import java.util.UUID;

import static com.hazelcast.config.cp.FencedLockConfig.DEFAULT_LOCK_ACQUIRE_LIMIT;
import static com.hazelcast.cp.internal.datastructures.lock.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;

/**
 * Contains {@link RaftLock} resources and manages wait timeouts
 * based on lock / unlock requests
 */
class RaftLockRegistry extends ResourceRegistry<LockInvocationKey, RaftLock> implements IdentifiedDataSerializable {

    private CPSubsystemConfig cpSubsystemConfig;

    RaftLockRegistry() {
    }

    RaftLockRegistry(CPSubsystemConfig cpSubsystemConfig, CPGroupId groupId) {
        super(groupId);
        this.cpSubsystemConfig = cpSubsystemConfig;
    }

    public void setCpSubsystemConfig(CPSubsystemConfig cpSubsystemConfig) {
        this.cpSubsystemConfig = cpSubsystemConfig;
    }

    @Override
    protected RaftLock createNewResource(CPGroupId groupId, String name) {
        FencedLockConfig lockConfig = cpSubsystemConfig.findLockConfig(name);
        int lockCountLimit = (lockConfig != null)
                ? lockConfig.getLockAcquireLimit() : DEFAULT_LOCK_ACQUIRE_LIMIT;

        return new RaftLock(groupId, name, lockCountLimit);
    }

    @Override
    protected RaftLockRegistry cloneForSnapshot() {
        RaftLockRegistry clone = new RaftLockRegistry();
        clone.groupId = this.groupId;
        for (Entry<String, RaftLock> e : this.resources.entrySet()) {
            clone.resources.put(e.getKey(), e.getValue().cloneForSnapshot());
        }
        clone.destroyedNames.addAll(this.destroyedNames);
        clone.waitTimeouts.putAll(this.waitTimeouts);

        return clone;
    }

    AcquireResult acquire(String name, LockInvocationKey key, long timeoutMs) {
        AcquireResult result = getOrInitResource(name).acquire(key, (timeoutMs != 0));

        for (LockInvocationKey cancelled : result.cancelledWaitKeys()) {
            removeWaitKey(name, cancelled);
        }

        if (result.status() == WAIT_KEY_ADDED) {
            addWaitKey(name, key, timeoutMs);
        }

        return result;
    }

    ReleaseResult release(String name, LockEndpoint endpoint, UUID invocationUid) {
        RaftLock lock = getResourceOrNull(name);
        if (lock == null) {
            return ReleaseResult.FAILED;
        }

        ReleaseResult result = lock.release(endpoint, invocationUid);
        for (LockInvocationKey key : result.completedWaitKeys()) {
            removeWaitKey(name, key);
        }

        return result;
    }

    RaftLockOwnershipState getLockOwnershipState(String name) {
        RaftLock lock = getResourceOrNull(name);
        return lock != null ? lock.lockOwnershipState() : RaftLockOwnershipState.NOT_LOCKED;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK_REGISTRY;
    }
}
