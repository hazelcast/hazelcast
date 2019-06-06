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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphore.AcquireResult;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphore.ReleaseResult;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Contains {@link RaftSemaphore} resources and manages wait timeouts
 * based on acquire / release requests
 */
public class RaftSemaphoreRegistry extends ResourceRegistry<AcquireInvocationKey, RaftSemaphore>
        implements IdentifiedDataSerializable {

    RaftSemaphoreRegistry() {
    }

    RaftSemaphoreRegistry(CPGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftSemaphore createNewResource(CPGroupId groupId, String name) {
        return new RaftSemaphore(groupId, name);
    }

    @Override
    protected RaftSemaphoreRegistry cloneForSnapshot() {
        RaftSemaphoreRegistry clone = new RaftSemaphoreRegistry();
        clone.groupId = this.groupId;
        for (Entry<String, RaftSemaphore> e : this.resources.entrySet()) {
            clone.resources.put(e.getKey(), e.getValue().cloneForSnapshot());
        }
        clone.destroyedNames.addAll(this.destroyedNames);
        clone.waitTimeouts.putAll(this.waitTimeouts);

        return clone;
    }

    Collection<AcquireInvocationKey> init(String name, int permits) {
        Collection<AcquireInvocationKey> acquired = getOrInitResource(name).init(permits);

        for (AcquireInvocationKey key : acquired) {
            removeWaitKey(name, key);
        }

        return acquired;
    }

    int availablePermits(String name) {
        RaftSemaphore semaphore = getResourceOrNull(name);
        return semaphore != null ? semaphore.getAvailable() : 0;
    }

    AcquireResult acquire(String name, AcquireInvocationKey key, long timeoutMs) {
        AcquireResult result = getOrInitResource(name).acquire(key, (timeoutMs != 0));

        for (AcquireInvocationKey waitKey : result.cancelled) {
            removeWaitKey(name, waitKey);
        }

        if (result.acquired == 0) {
            addWaitKey(name, key, timeoutMs);
        }

        return result;
    }

    ReleaseResult release(String name, SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        ReleaseResult result = getOrInitResource(name).release(endpoint, invocationUid, permits);
        for (AcquireInvocationKey key : result.acquired) {
            removeWaitKey(name, key);
        }

        for (AcquireInvocationKey key : result.cancelled) {
            removeWaitKey(name, key);
        }

        return result;
    }

    AcquireResult drainPermits(String name, SemaphoreEndpoint endpoint, UUID invocationUid) {
        AcquireResult result = getOrInitResource(name).drain(endpoint, invocationUid);
        for (AcquireInvocationKey key : result.cancelled) {
            removeWaitKey(name, key);
        }

        return result;
    }

    ReleaseResult changePermits(String name, SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        ReleaseResult result = getOrInitResource(name).change(endpoint, invocationUid, permits);
        for (AcquireInvocationKey key : result.acquired) {
            removeWaitKey(name, key);
        }

        for (AcquireInvocationKey key : result.cancelled) {
            removeWaitKey(name, key);
        }

        return result;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftSemaphoreDataSerializerHook.RAFT_SEMAPHORE_REGISTRY;
    }
}
