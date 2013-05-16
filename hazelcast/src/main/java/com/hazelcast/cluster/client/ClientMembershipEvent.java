package com.hazelcast.cluster.client;

import com.hazelcast.cluster.ClusterDataSerializerHook;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @mdogan 5/15/13
 */
public final class ClientMembershipEvent implements IdentifiedDataSerializable {

    private MemberImpl member;
    private boolean added;

    public ClientMembershipEvent() {
    }

    public ClientMembershipEvent(MemberImpl member, boolean added) {
        this.member = member;
        this.added = added;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBERSHIP_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(added);
        member.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        added = in.readBoolean();
        member = new MemberImpl();
        member.readData(in);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientMembershipEvent{");
        sb.append("member=").append(member);
        sb.append(", added=").append(added);
        sb.append('}');
        return sb.toString();
    }

    public boolean isAdded() {
        return added;
    }

    public MemberImpl getMember() {
        return member;
    }
}
