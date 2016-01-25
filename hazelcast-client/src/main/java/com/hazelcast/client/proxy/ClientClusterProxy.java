/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.transaction.TransactionOptions;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Client implementation of the {@link com.hazelcast.core.Cluster}.
 */
public class ClientClusterProxy implements Cluster {

    private final ClientClusterServiceImpl clusterService;

    public ClientClusterProxy(ClientClusterServiceImpl clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String addMembershipListener(MembershipListener listener) {
        return clusterService.addMembershipListener(listener);
    }

    @Override
    public boolean removeMembershipListener(String registrationId) {
        return clusterService.removeMembershipListener(registrationId);
    }

    @Override
    public Set<Member> getMembers() {
        final Collection<Member> members = clusterService.getMemberList();
        return new LinkedHashSet<Member>(members);
    }

    @Override
    public Member getLocalMember() {
        throw new UnsupportedOperationException("Client has no local member!");
    }

    @Override
    public long getClusterTime() {
        return clusterService.getClusterTime();
    }

    @Override
    public ClusterState getClusterState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterState(ClusterState newState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterState(ClusterState newState, TransactionOptions transactionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown(TransactionOptions transactionOptions) {
        throw new UnsupportedOperationException();
    }
}
