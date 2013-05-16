package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.util.Clock;

import java.util.Set;

/**
 * @mdogan 5/15/13
 */
public class ClusterProxy implements Cluster {

    private final ClientClusterService clusterService;

    public ClusterProxy(ClientClusterService clusterService) {
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
        return clusterService.getMembers();
    }

    @Override
    public Member getLocalMember() {
        throw new UnsupportedOperationException("Client has no local member!");
    }

    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis();
    }
}
