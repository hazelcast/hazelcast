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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Provides access to CP subsystem utilities
 */
public class CPSubsystemImpl implements CPSubsystem {

    private final HazelcastInstanceImpl instance;
    private final boolean cpSubsystemEnabled;

    public CPSubsystemImpl(HazelcastInstanceImpl instance) {
        this.instance = instance;
        int cpMemberCount = instance.getConfig().getCPSubsystemConfig().getCPMemberCount();
        this.cpSubsystemEnabled = cpMemberCount > 0;
        ILogger logger = instance.node.getLogger(CPSubsystem.class);
        if (cpSubsystemEnabled) {
            logger.info("CP Subsystem is enabled with " + cpMemberCount + " members.");
        } else {
            logger.warning("CP Subsystem is not enabled. CP data structures will operate in UNSAFE mode! "
                    + "Please note that UNSAFE mode will not provide strong consistency guarantees.");
        }
    }

    @Override
    public IAtomicLong getAtomicLong(String name) {
        checkNotNull(name, "Retrieving an atomic long instance with a null name is not allowed!");
        return createProxy(RaftAtomicLongService.SERVICE_NAME, name);
    }

    @Override
    public <E> IAtomicReference<E> getAtomicReference(String name) {
        checkNotNull(name, "Retrieving an atomic reference instance with a null name is not allowed!");
        return createProxy(AtomicRefService.SERVICE_NAME, name);
    }

    @Override
    public ICountDownLatch getCountDownLatch(String name) {
        checkNotNull(name, "Retrieving a count down latch instance with a null name is not allowed!");
        return createProxy(CountDownLatchService.SERVICE_NAME, name);
    }

    @Override
    public FencedLock getLock(String name) {
        checkNotNull(name, "Retrieving an fenced lock instance with a null name is not allowed!");
        return createProxy(RaftLockService.SERVICE_NAME, name);
    }

    @Override
    public ISemaphore getSemaphore(String name) {
        checkNotNull(name, "Retrieving a semaphore instance with a null name is not allowed!");
        return createProxy(SemaphoreService.SERVICE_NAME, name);
    }

    @Override
    public CPMember getLocalCPMember() {
        return getCPSubsystemManagementService().getLocalCPMember();
    }

    @Override
    public CPSubsystemManagementService getCPSubsystemManagementService() {
        if (!cpSubsystemEnabled) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }
        return getService(RaftService.SERVICE_NAME);
    }

    @Override
    public CPSessionManagementService getCPSessionManagementService() {
        if (!cpSubsystemEnabled) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }
        return getService(RaftSessionService.SERVICE_NAME);
    }

    private <T> T getService(String serviceName) {
        return instance.node.getNodeEngine().getService(serviceName);
    }

    private <T extends DistributedObject> T createProxy(String serviceName, String name) {
        RaftRemoteService service = getService(serviceName);
        return service.createProxy(name);
    }

}
