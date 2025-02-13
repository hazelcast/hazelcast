/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSessionManagementService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

public class CPSubsystemStubImpl implements CPSubsystem, Consumer<ClientContext> {
    /**
     * Server-side message to inform that CP subsystem is not available in Hazelcast OS members
     */
    public static final String CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_MEMBERS =
            "CP subsystem is a licensed feature. Please ensure you have an Enterprise license that enables CP.";
    /**
     * Client-side message to inform that CP subsystem is not available in Hazelcast OS clients
     */
    public static final String CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_CLIENTS =
            "CP subsystem is an Enterprise feature. Please use the latest applicable Hazelcast Enterprise client to interact "
                    + "with Enterprise clusters using CP.";

    private final String cpSubsystemNotAvailableMessage;

    /**
     * Creates a stub for the CP Subsystem, used in the OS implementation.
     *
     * @param implementorIsClient whether the implementor of this stub is a client or a member, which
     *                            determines which message is displayed in thrown exceptions.
     */
    public CPSubsystemStubImpl(boolean implementorIsClient) {
        this.cpSubsystemNotAvailableMessage = implementorIsClient ? CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_CLIENTS
                : CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_MEMBERS;
    }

    @Nonnull
    @Override
    public IAtomicLong getAtomicLong(@Nonnull String name) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Nonnull
    @Override
    public <E> IAtomicReference<E> getAtomicReference(@Nonnull String name) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Nonnull
    @Override
    public ICountDownLatch getCountDownLatch(@Nonnull String name) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Nonnull
    @Override
    public FencedLock getLock(@Nonnull String name) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Nonnull
    @Override
    public ISemaphore getSemaphore(@Nonnull String name) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Override
    public CPMember getLocalCPMember() {
        return null;
    }

    @Override
    public CPSubsystemManagementService getCPSubsystemManagementService() {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Override
    public CPSessionManagementService getCPSessionManagementService() {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Override
    public UUID addMembershipListener(CPMembershipListener listener) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Override
    public boolean removeMembershipListener(UUID id) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Override
    public UUID addGroupAvailabilityListener(CPGroupAvailabilityListener listener) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Override
    public boolean removeGroupAvailabilityListener(UUID id) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Nonnull
    @Override
    public <K, V> CPMap<K, V> getMap(@Nonnull String name) {
        throw new UnsupportedOperationException(cpSubsystemNotAvailableMessage);
    }

    @Nonnull
    @Override
    public Collection<CPGroupId> getCPGroupIds() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Iterable<CPObjectInfo> getObjectInfos(@Nonnull CPGroupId groupId, @Nonnull String serviceName) {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Iterable<CPObjectInfo> getTombstoneInfos(@Nonnull CPGroupId groupId, @Nonnull String serviceName) {
        return Collections.emptyList();
    }

    @Override
    public void accept(ClientContext clientContext) {
    }
}
