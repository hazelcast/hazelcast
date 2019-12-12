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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Client implementation of the {@link Cluster}.
 */
public class ClientClusterProxy implements Cluster {

    private final ClientClusterServiceImpl clusterService;

    public ClientClusterProxy(ClientClusterServiceImpl clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public UUID addMembershipListener(@Nonnull MembershipListener listener) {
        return clusterService.addMembershipListener(listener);
    }

    @Override
    public boolean removeMembershipListener(@Nonnull UUID registrationId) {
        return clusterService.removeMembershipListener(registrationId);
    }

    @Override
    public Set<Member> getMembers() {
        final Collection<Member> members = clusterService.getMemberList();
        return new LinkedHashSet<>(members);
    }

    @Override
    public Member getLocalMember() {
        throw new UnsupportedOperationException("Client has no local member!");
    }

    @Override
    public long getClusterTime() {
        return clusterService.getClusterTime();
    }

    @Nonnull
    @Override
    public ClusterState getClusterState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterState(@Nonnull ClusterState newState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Version getClusterVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HotRestartService getHotRestartService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterState(@Nonnull ClusterState newState, @Nonnull TransactionOptions transactionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown(@Nullable TransactionOptions transactionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterVersion(@Nonnull Version version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterVersion(@Nonnull Version version, @Nonnull TransactionOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void promoteLocalLiteMember() {
        throw new UnsupportedOperationException();
    }
}
