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
     * Message to inform that CP subsystem is not available in Hazelcast OS
     */
    public static final String CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS =
            "CP subsystem is a licensed feature. Please ensure you have an Enterprise license that enables CP.";

    @Nonnull
    @Override
    public IAtomicLong getAtomicLong(@Nonnull String name) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Nonnull
    @Override
    public <E> IAtomicReference<E> getAtomicReference(@Nonnull String name) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Nonnull
    @Override
    public ICountDownLatch getCountDownLatch(@Nonnull String name) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Nonnull
    @Override
    public FencedLock getLock(@Nonnull String name) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Nonnull
    @Override
    public ISemaphore getSemaphore(@Nonnull String name) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Override
    public CPMember getLocalCPMember() {
        return null;
    }

    @Override
    public CPSubsystemManagementService getCPSubsystemManagementService() {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Override
    public CPSessionManagementService getCPSessionManagementService() {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Override
    public UUID addMembershipListener(CPMembershipListener listener) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Override
    public boolean removeMembershipListener(UUID id) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Override
    public UUID addGroupAvailabilityListener(CPGroupAvailabilityListener listener) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Override
    public boolean removeGroupAvailabilityListener(UUID id) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
    }

    @Nonnull
    @Override
    public <K, V> CPMap<K, V> getMap(@Nonnull String name) {
        throw new UnsupportedOperationException(CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS);
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
