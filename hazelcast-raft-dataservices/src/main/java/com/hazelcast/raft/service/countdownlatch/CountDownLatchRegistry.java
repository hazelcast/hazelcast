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

package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.ResourceRegistry;

import java.util.Collection;
import java.util.UUID;

/**
 * Contains {@link RaftCountDownLatch} resources and manages wait timeouts
 */
public class CountDownLatchRegistry extends ResourceRegistry<CountDownLatchInvocationKey, RaftCountDownLatch>
        implements IdentifiedDataSerializable {

    CountDownLatchRegistry() {
    }

    CountDownLatchRegistry(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftCountDownLatch createNewResource(RaftGroupId groupId, String name) {
        return new RaftCountDownLatch(groupId, name);
    }

    boolean trySetCount(String name, int count) {
        return getOrInitResource(name).trySetCount(count);
    }

    Tuple2<Integer, Collection<CountDownLatchInvocationKey>> countDown(String name, int expectedRound, UUID invocationUuid) {
        RaftCountDownLatch latch = getOrInitResource(name);
        Tuple2<Integer, Collection<CountDownLatchInvocationKey>> t = latch.countDown(expectedRound, invocationUuid);
        for (CountDownLatchInvocationKey key : t.element2) {
            removeWaitKey(key);
        }

        return t;
    }

    boolean await(String name, long commitIndex, long timeoutMs) {
        boolean success = getOrInitResource(name).await(commitIndex, timeoutMs > 0);
        if (!success) {
            addWaitKey(new CountDownLatchInvocationKey(name, commitIndex), timeoutMs);
        }

        return success;
    }

    int getRemainingCount(String name) {
        return getOrInitResource(name).getRemainingCount();
    }

    int getRound(String name) {
        return getOrInitResource(name).getRound();
    }

    @Override
    public int getFactoryId() {
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_REGISTRY;
    }
}
