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

package com.hazelcast.client.cp.internal;

import com.hazelcast.client.cp.internal.datastructures.proxy.ClientRaftProxyFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveMembershipListenerCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.event.CPGroupAvailabilityEvent;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipEvent.EventType;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.event.impl.CPGroupAvailabilityEventImpl;
import com.hazelcast.cp.event.impl.CPMembershipEventImpl;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.internal.util.Clock;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Client-side impl of the CP subsystem to create CP data structure proxies
 */
public class CPSubsystemImpl implements CPSubsystem {

    private final ClientRaftProxyFactory proxyFactory;
    private volatile ClientContext context;

    public CPSubsystemImpl(HazelcastClientInstanceImpl client) {
        this.proxyFactory = new ClientRaftProxyFactory(client);
    }

    public void init(ClientContext context) {
        this.context = context;
        proxyFactory.init(context);
    }

    @Nonnull
    @Override
    public IAtomicLong getAtomicLong(@Nonnull String name) {
        checkNotNull(name, "Retrieving an atomic long instance with a null name is not allowed!");
        return proxyFactory.createProxy(AtomicLongService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> IAtomicReference<E> getAtomicReference(@Nonnull String name) {
        checkNotNull(name, "Retrieving an atomic reference instance with a null name is not allowed!");
        return proxyFactory.createProxy(AtomicRefService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public ICountDownLatch getCountDownLatch(@Nonnull String name) {
        checkNotNull(name, "Retrieving a count down latch instance with a null name is not allowed!");
        return proxyFactory.createProxy(CountDownLatchService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public FencedLock getLock(@Nonnull String name) {
        checkNotNull(name, "Retrieving an fenced lock instance with a null name is not allowed!");
        return proxyFactory.createProxy(LockService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public ISemaphore getSemaphore(@Nonnull String name) {
        checkNotNull(name, "Retrieving a semaphore instance with a null name is not allowed!");
        return proxyFactory.createProxy(SemaphoreService.SERVICE_NAME, name);
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

    @Override
    public UUID addMembershipListener(CPMembershipListener listener) {
        return context.getListenerService()
                .registerListener(new CPMembershipListenerMessageCodec(), new CPMembershipEventHandler(listener));
    }

    @Override
    public boolean removeMembershipListener(UUID id) {
        return context.getListenerService().deregisterListener(id);
    }

    @Override
    public UUID addGroupAvailabilityListener(CPGroupAvailabilityListener listener) {
        return context.getListenerService()
                .registerListener(new CPGroupAvailabilityListenerMessageCodec(), new CPGroupAvailabilityEventHandler(listener));
    }

    @Override
    public boolean removeGroupAvailabilityListener(UUID id) {
        return context.getListenerService().deregisterListener(id);
    }

    private static class CPMembershipEventHandler extends CPSubsystemAddMembershipListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final CPMembershipListener listener;
        CPMembershipEventHandler(CPMembershipListener listener) {
            this.listener = listener;
        }

        @Override
        public void handleMembershipEventEvent(CPMember member, byte type) {
            CPMembershipEvent event = new CPMembershipEventImpl(member, type);
            if (event.getType() == EventType.ADDED) {
                listener.memberAdded(event);
            } else {
                listener.memberRemoved(event);
            }
        }
    }

    private static class CPGroupAvailabilityEventHandler extends CPSubsystemAddGroupAvailabilityListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private static final long DEDUPLICATION_PERIOD = TimeUnit.MINUTES.toMillis(1);
        private final CPGroupAvailabilityListener listener;
        private final Map<CPGroupAvailabilityEvent, Long> recentEvents = new ConcurrentHashMap<>();

        CPGroupAvailabilityEventHandler(CPGroupAvailabilityListener listener) {
            this.listener = listener;
        }

        @Override
        public void handleGroupAvailabilityEventEvent(RaftGroupId groupId, Collection<CPMember> members,
                Collection<CPMember> unavailableMembers) {

            long now = Clock.currentTimeMillis();
            recentEvents.values().removeIf(expirationTime -> expirationTime < now);

            CPGroupAvailabilityEvent event = new CPGroupAvailabilityEventImpl(groupId, members, unavailableMembers);
            if (recentEvents.putIfAbsent(event, now + DEDUPLICATION_PERIOD) != null) {
                return;
            }

            if (event.isMajorityAvailable()) {
                listener.availabilityDecreased(event);
            } else {
                listener.majorityLost(event);
            }
        }
    }

    private static class CPMembershipListenerMessageCodec implements ListenerMessageCodec {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return CPSubsystemAddMembershipListenerCodec.encodeRequest(localOnly);
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return CPSubsystemAddMembershipListenerCodec.decodeResponse(clientMessage);
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return CPSubsystemRemoveMembershipListenerCodec.encodeRequest(realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return CPSubsystemRemoveMembershipListenerCodec.decodeResponse(clientMessage);
        }
    }

    private static class CPGroupAvailabilityListenerMessageCodec implements ListenerMessageCodec {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return CPSubsystemAddGroupAvailabilityListenerCodec.encodeRequest(localOnly);
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return CPSubsystemAddGroupAvailabilityListenerCodec.decodeResponse(clientMessage);
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return CPSubsystemRemoveGroupAvailabilityListenerCodec.encodeRequest(realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return CPSubsystemRemoveGroupAvailabilityListenerCodec.decodeResponse(clientMessage);
        }
    }
}
