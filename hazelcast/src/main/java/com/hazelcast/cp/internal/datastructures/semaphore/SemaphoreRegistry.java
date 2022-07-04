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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.UUID;

import static com.hazelcast.config.cp.SemaphoreConfig.DEFAULT_INITIAL_PERMITS;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;

/**
 * Contains {@link Semaphore} resources and manages wait timeouts
 * based on acquire / release requests
 */
public class SemaphoreRegistry extends ResourceRegistry<AcquireInvocationKey, Semaphore>
        implements IdentifiedDataSerializable {

    private CPSubsystemConfig cpSubsystemConfig;

    SemaphoreRegistry() {
    }

    SemaphoreRegistry(CPGroupId groupId, CPSubsystemConfig cpSubsystemConfig) {
        super(groupId);
        this.cpSubsystemConfig = cpSubsystemConfig;
    }

    public void setCpSubsystemConfig(CPSubsystemConfig cpSubsystemConfig) {
        this.cpSubsystemConfig = cpSubsystemConfig;
    }

    @Override
    protected Semaphore createNewResource(CPGroupId groupId, String name) {
        SemaphoreConfig semaphoreConfig = cpSubsystemConfig.findSemaphoreConfig(name);
        int initialPermits = semaphoreConfig != null ? semaphoreConfig.getInitialPermits() : DEFAULT_INITIAL_PERMITS;
        return new Semaphore(groupId, name, initialPermits);
    }

    @Override
    protected SemaphoreRegistry cloneForSnapshot() {
        SemaphoreRegistry clone = new SemaphoreRegistry();
        clone.groupId = this.groupId;
        for (Entry<String, Semaphore> e : this.resources.entrySet()) {
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
        return getOrInitResource(name).getAvailable();
    }

    AcquireResult acquire(String name, AcquireInvocationKey key, long timeoutMs) {
        AcquireResult result = getOrInitResource(name).acquire(key, (timeoutMs != 0));

        for (AcquireInvocationKey waitKey : result.cancelledWaitKeys()) {
            removeWaitKey(name, waitKey);
        }

        if (result.status() == WAIT_KEY_ADDED) {
            addWaitKey(name, key, timeoutMs);
        }

        return result;
    }

    ReleaseResult release(String name, SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        ReleaseResult result = getOrInitResource(name).release(endpoint, invocationUid, permits);
        for (AcquireInvocationKey key : result.acquiredWaitKeys()) {
            removeWaitKey(name, key);
        }

        for (AcquireInvocationKey key : result.cancelledWaitKeys()) {
            removeWaitKey(name, key);
        }

        return result;
    }

    AcquireResult drainPermits(String name, SemaphoreEndpoint endpoint, UUID invocationUid) {
        AcquireResult result = getOrInitResource(name).drain(endpoint, invocationUid);
        for (AcquireInvocationKey key : result.cancelledWaitKeys()) {
            removeWaitKey(name, key);
        }

        return result;
    }

    ReleaseResult changePermits(String name, SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        ReleaseResult result = getOrInitResource(name).change(endpoint, invocationUid, permits);
        for (AcquireInvocationKey key : result.acquiredWaitKeys()) {
            removeWaitKey(name, key);
        }

        for (AcquireInvocationKey key : result.cancelledWaitKeys()) {
            removeWaitKey(name, key);
        }

        return result;
    }

    Collection<Semaphore> getAllSemaphores() {
        return resources.values();
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.RAFT_SEMAPHORE_REGISTRY;
    }
}
