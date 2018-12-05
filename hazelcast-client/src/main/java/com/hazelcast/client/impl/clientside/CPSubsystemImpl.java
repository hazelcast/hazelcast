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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;

import static com.hazelcast.cp.internal.RaftService.withoutDefaultGroupName;

/**
 * TODO [basri] javadoc
 */
class CPSubsystemImpl implements CPSubsystem {

    private final HazelcastClientInstanceImpl client;

    CPSubsystemImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        return client.getDistributedObject(RaftAtomicLongService.SERVICE_NAME, withoutDefaultGroupName(name));
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        return client.getDistributedObject(RaftAtomicRefService.SERVICE_NAME, withoutDefaultGroupName(name));
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        return client.getDistributedObject(RaftCountDownLatchService.SERVICE_NAME, withoutDefaultGroupName(name));
    }

    @Override
    public FencedLock getLock(String name) {
        return client.getDistributedObject(RaftLockService.SERVICE_NAME, withoutDefaultGroupName(name));
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        return client.getDistributedObject(RaftSemaphoreService.SERVICE_NAME, withoutDefaultGroupName(name));
    }

    @Override
    public CPMember getLocalCPMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemManagementService getCPSubsystemManagementService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSessionManagementService getCPSessionManagementService() {
        throw new UnsupportedOperationException();
    }
}
