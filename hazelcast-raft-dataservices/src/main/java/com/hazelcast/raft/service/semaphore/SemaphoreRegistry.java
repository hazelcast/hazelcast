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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.blocking.ResourceRegistry;
import com.hazelcast.raft.service.semaphore.RaftSemaphore.AcquireResult;
import com.hazelcast.raft.service.semaphore.RaftSemaphore.ReleaseResult;

import java.util.Collection;
import java.util.UUID;

/**
 * Contains {@link RaftSemaphore} resources and manages wait timeouts based on acquire / release requests
 */
public class SemaphoreRegistry extends ResourceRegistry<SemaphoreInvocationKey, RaftSemaphore>
        implements IdentifiedDataSerializable {

    SemaphoreRegistry() {
    }

    SemaphoreRegistry(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftSemaphore createNewResource(RaftGroupId groupId, String name) {
        return new RaftSemaphore(groupId, name);
    }

    Collection<SemaphoreInvocationKey> init(String name, int permits) {
        Collection<SemaphoreInvocationKey> acquired = getOrInitResource(name).init(permits);

        for (SemaphoreInvocationKey waitKey : acquired) {
            removeWaitKey(waitKey);
        }

        return acquired;
    }

    int availablePermits(String name) {
        RaftSemaphore semaphore = getResourceOrNull(name);
        return semaphore != null ? semaphore.getAvailable() : 0;
    }

    AcquireResult acquire(String name, SemaphoreInvocationKey key, long timeoutMs) {
        AcquireResult result = getOrInitResource(name).acquire(key, (timeoutMs != 0));

        for (SemaphoreInvocationKey waitKey : result.cancelled) {
            removeWaitKey(waitKey);
        }

        if (result.acquired == 0 && timeoutMs > 0) {
            addWaitKey(key, timeoutMs);
        }

        return result;
    }

    ReleaseResult release(String name, long sessionId, long threadId, UUID invocationUid, int permits) {
        ReleaseResult result = getOrInitResource(name).release(sessionId, threadId, invocationUid, permits);
        for (SemaphoreInvocationKey key : result.acquired) {
            removeWaitKey(key);
        }

        for (SemaphoreInvocationKey key : result.cancelled) {
            removeWaitKey(key);
        }

        return result;
    }

    AcquireResult drainPermits(String name, long sessionId, long threadId, UUID invocationUid) {
        AcquireResult result = getOrInitResource(name).drain(sessionId, threadId, invocationUid);
        for (SemaphoreInvocationKey waitKey : result.cancelled) {
            removeWaitKey(waitKey);
        }

        return result;
    }

    ReleaseResult changePermits(String name, long sessionId, long threadId, UUID invocationUid, int permits) {
        ReleaseResult result = getOrInitResource(name).change(sessionId, threadId, invocationUid, permits);
        for (SemaphoreInvocationKey key : result.acquired) {
            removeWaitKey(key);
        }

        for (SemaphoreInvocationKey key : result.cancelled) {
            removeWaitKey(key);
        }

        return result;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.SEMAPHORE_REGISTRY;
    }
}
