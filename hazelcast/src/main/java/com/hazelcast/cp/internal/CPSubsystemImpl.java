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

package com.hazelcast.cp.internal;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.instance.HazelcastInstanceImpl;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Provides access to CP Subsystem utilities
 */
public class CPSubsystemImpl implements CPSubsystem {

    private final HazelcastInstanceImpl instance;

    public CPSubsystemImpl(HazelcastInstanceImpl instance) {
        this.instance = instance;
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        checkNotNull(name, "Retrieving an atomic long instance with a null name is not allowed!");
        RaftRemoteService service = getService(RaftAtomicLongService.SERVICE_NAME);
        return service.createProxy(name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        checkNotNull(name, "Retrieving an atomic reference instance with a null name is not allowed!");
        RaftRemoteService service = getService(RaftAtomicRefService.SERVICE_NAME);
        return service.createProxy(name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        checkNotNull(name, "Retrieving a count down latch instance with a null name is not allowed!");
        RaftRemoteService service = getService(RaftCountDownLatchService.SERVICE_NAME);
        return service.createProxy(name);
    }

    @Override
    public FencedLock getLock(String name) {
        checkNotNull(name, "Retrieving an fenced lock instance with a null name is not allowed!");
        RaftRemoteService service = getService(RaftLockService.SERVICE_NAME);
        return service.createProxy(name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        checkNotNull(name, "Retrieving a semaphore instance with a null name is not allowed!");
        RaftRemoteService service = getService(RaftSemaphoreService.SERVICE_NAME);
        return service.createProxy(name);
    }

    @Override
    public CPMember getLocalCPMember() {
        return getCPSubsystemManagementService().getLocalCPMember();
    }

    @Override
    public CPSubsystemManagementService getCPSubsystemManagementService() {
        if (instance.getConfig().getCPSubsystemConfig().getCPMemberCount() == 0) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }
        return instance.node.getNodeEngine().getService(RaftService.SERVICE_NAME);
    }

    @Override
    public CPSessionManagementService getCPSessionManagementService() {
        if (instance.getConfig().getCPSubsystemConfig().getCPMemberCount() == 0) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }
        return instance.node.getNodeEngine().getService(RaftSessionService.SERVICE_NAME);
    }

    private <T> T getService(String serviceName) {
        return instance.node.getNodeEngine().getService(serviceName);
    }

}
