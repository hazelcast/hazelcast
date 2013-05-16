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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @mdogan 5/13/13
 */
public final class AddMembershipListenerRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    @Override
    public Object call() throws Exception {
        ClusterServiceImpl service = getService();
        service.addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                final MemberImpl member = (MemberImpl) membershipEvent.getMember();
                getClientEngine().sendResponse(getEndpoint(), new ClientMembershipEvent(member, true));
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                final MemberImpl member = (MemberImpl) membershipEvent.getMember();
                getClientEngine().sendResponse(getEndpoint(), new ClientMembershipEvent(member, false));
            }
        });

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
