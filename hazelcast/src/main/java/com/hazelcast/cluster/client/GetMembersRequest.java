package com.hazelcast.cluster.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.cluster.ClusterDataSerializerHook;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;

/**
 * @mdogan 5/13/13
 */
public final class GetMembersRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    @Override
    public Object call() throws Exception {
        ClusterServiceImpl service = getService();
        final Collection<MemberImpl> members = service.getMemberList();
        return null;
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
        return ClusterDataSerializerHook.GET_MEMBERS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
