package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

// TODO [basri] ADD JAVADOC
public class FetchMemberListStateOperation extends AbstractClusterOperation implements JoinOperation {

    private String masterUuid;

    private MembersView membersView;


    public FetchMemberListStateOperation() {
    }

    public FetchMemberListStateOperation(String masterUuid) {
        this.masterUuid = masterUuid;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        membersView = service.handleMastershipClaim(getCallerAddress(), masterUuid);
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

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(masterUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        masterUuid = in.readUTF();
    }

}
