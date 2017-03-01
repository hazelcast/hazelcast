package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;

// TODO [basri] ADD JAVADOC
public class FetchMemberListStateOperation extends AbstractClusterOperation implements JoinOperation {

    private MembersView membersView;

    public FetchMemberListStateOperation() {
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        membersView = service.handleMastershipClaim(getCallerAddress(), getCallerUuid());
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return membersView;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.FETCH_MEMBER_LIST_STATE;
    }

}
