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

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.AbstractBlockingService;
import com.hazelcast.raft.service.countdownlatch.proxy.RaftCountDownLatchProxy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.raft.impl.service.RaftService.getObjectNameForProxy;

/**
 * Contains Raft-based count down latch instances
 */
public class RaftCountDownLatchService
        extends AbstractBlockingService<CountDownLatchInvocationKey, RaftCountDownLatch, CountDownLatchRegistry> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:countDownLatchService";

    public RaftCountDownLatchService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public ICountDownLatch createRaftObjectProxy(String name) {
        try {
            RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
            RaftGroupId groupId = service.createRaftGroupForProxy(name);
            return new RaftCountDownLatchProxy(raftService.getInvocationManager(), groupId, getObjectNameForProxy(name));
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean trySetCount(RaftGroupId groupId, String name, int count) {
        return getOrInitRegistry(groupId).trySetCount(name, count);
    }

    public int countDown(RaftGroupId groupId, String name, int expectedRound, UUID invocationUuid) {
        CountDownLatchRegistry registry = getOrInitRegistry(groupId);
        Tuple2<Integer, Collection<CountDownLatchInvocationKey>> t = registry.countDown(name, expectedRound, invocationUuid);
        notifyWaitKeys(groupId, t.element2, true);

        return t.element1;
    }

    public boolean await(RaftGroupId groupId, String name, long commitIndex, long timeoutMillis) {
        boolean success = getOrInitRegistry(groupId).await(name, commitIndex, timeoutMillis);
        if (!success) {
            scheduleTimeout(groupId, new CountDownLatchInvocationKey(name, commitIndex), timeoutMillis);
        }

        return success;
    }

    public int getRemainingCount(RaftGroupId groupId, String name) {
        return getOrInitRegistry(groupId).getRemainingCount(name);
    }

    public int getRound(RaftGroupId groupId, String name) {
        return getOrInitRegistry(groupId).getRound(name);
    }

    @Override
    protected CountDownLatchRegistry createNewRegistry(RaftGroupId groupId) {
        return new CountDownLatchRegistry(groupId);
    }

    @Override
    protected Object expiredWaitKeyResponse() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }
}
