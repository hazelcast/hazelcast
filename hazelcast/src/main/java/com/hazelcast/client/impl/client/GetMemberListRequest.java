package com.hazelcast.client.impl.client;

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.ClientService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializableCollection;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Returns member list to client
 */
public class GetMemberListRequest extends CallableClientRequest implements RetryableRequest {

    public GetMemberListRequest() {
    }

    @Override
    public Object call() throws Exception {
        ClusterService service = getService();

        Collection<MemberImpl> memberList = service.getMemberList();
        Collection<Data> response = new ArrayList<Data>(memberList.size());
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
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.GET_MEMBER_LIST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
