package com.hazelcast.cluster.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.cluster.ClusterDataSerializerHook;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.MutableString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @mdogan 5/13/13
 */
public final class AddMembershipListenerRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    @Override
    public Object call() throws Exception {
        final ClusterServiceImpl service = getService();
        final MutableString id = new MutableString();
        final String registration = service.addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                if (getEndpoint().live()) {
                    final MemberImpl member = (MemberImpl) membershipEvent.getMember();
                    getClientEngine().sendResponse(getEndpoint(), new ClientMembershipEvent(member, MembershipEvent.MEMBER_ADDED));
                } else {
                    deregister();
                }
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                if (getEndpoint().live()) {
                    final MemberImpl member = (MemberImpl) membershipEvent.getMember();
                    getClientEngine().sendResponse(getEndpoint(), new ClientMembershipEvent(member, MembershipEvent.MEMBER_REMOVED));
                } else {
                    deregister();
                }
            }

            private void deregister() {
                final String registrationId = id.getString();
                if (registrationId != null) {
                    service.removeMembershipListener(registrationId);
                }
            }
        });
        id.setString(registration);

        final Collection<MemberImpl> memberList = service.getMemberList();
        final Collection<Data> response = new ArrayList<Data>(memberList.size());
        final SerializationService serializationService = getClientEngine().getSerializationService();
        for (MemberImpl member : memberList) {
            response.add(serializationService.toData(member));
        }
        return new SerializableCollection(response);
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.ADD_MS_LISTENER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
