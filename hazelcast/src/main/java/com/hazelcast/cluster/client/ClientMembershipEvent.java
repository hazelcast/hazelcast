package com.hazelcast.cluster.client;

import com.hazelcast.cluster.ClusterDataSerializerHook;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @mdogan 5/15/13
 */
public final class ClientMembershipEvent extends MembershipEvent implements IdentifiedDataSerializable {

    public ClientMembershipEvent() {
    }

    public ClientMembershipEvent(Member member, int eventType) {
        super(member, eventType);
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBERSHIP_EVENT;
    }
}
