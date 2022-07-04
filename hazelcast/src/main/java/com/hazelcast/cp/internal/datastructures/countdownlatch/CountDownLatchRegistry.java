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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Contains {@link CountDownLatch} resources and manages wait timeouts
 */
public class CountDownLatchRegistry extends ResourceRegistry<AwaitInvocationKey, CountDownLatch>
        implements IdentifiedDataSerializable {

    CountDownLatchRegistry() {
    }

    CountDownLatchRegistry(CPGroupId groupId) {
        super(groupId);
    }

    @Override
    protected CountDownLatch createNewResource(CPGroupId groupId, String name) {
        return new CountDownLatch(groupId, name);
    }

    @Override
    protected CountDownLatchRegistry cloneForSnapshot() {
        CountDownLatchRegistry clone = new CountDownLatchRegistry();
        clone.groupId = this.groupId;
        for (Entry<String, CountDownLatch> e : this.resources.entrySet()) {
            clone.resources.put(e.getKey(), e.getValue().cloneForSnapshot());
        }
        clone.destroyedNames.addAll(this.destroyedNames);
        clone.waitTimeouts.putAll(this.waitTimeouts);

        return clone;
    }

    boolean trySetCount(String name, int count) {
        return getOrInitResource(name).trySetCount(count);
    }

    BiTuple<Integer, Collection<AwaitInvocationKey>> countDown(String name, UUID invocationUuid, int expectedRound) {
        CountDownLatch latch = getOrInitResource(name);
        BiTuple<Integer, Collection<AwaitInvocationKey>> t = latch.countDown(invocationUuid, expectedRound);
        for (AwaitInvocationKey key : t.element2) {
            removeWaitKey(name, key);
        }

        return t;
    }

    boolean await(String name, AwaitInvocationKey key, long timeoutMs) {
        boolean success = getOrInitResource(name).await(key, (timeoutMs > 0));
        if (!success) {
            addWaitKey(name, key, timeoutMs);
        }

        return success;
    }

    int getRemainingCount(String name) {
        return getOrInitResource(name).getRemainingCount();
    }

    int getRound(String name) {
        return getOrInitResource(name).getRound();
    }

    Collection<CountDownLatch> getAllLatches() {
        return resources.values();
    }

    @Override
    public int getFactoryId() {
        return CountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_REGISTRY;
    }
}
