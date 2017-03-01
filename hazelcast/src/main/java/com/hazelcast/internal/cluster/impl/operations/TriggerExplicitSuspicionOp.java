package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.TRIGGER_EXPLICIT_SUSPICION;

// TODO [basri] ADD JAVADOC
public class TriggerExplicitSuspicionOp extends AbstractClusterOperation {

    private int callerMemberListVersion;

    private Address endpoint;

    private Address endpointMasterAddress;

    private int endpointMemberListVersion;

    public TriggerExplicitSuspicionOp(int callerMemberListVersion, Address endpoint, Address endpointMasterAddress,
                                      int endpointMemberListVersion) {
        this.callerMemberListVersion = callerMemberListVersion;
        this.endpoint = endpoint;
        this.endpointMasterAddress = endpointMasterAddress;
        this.endpointMemberListVersion = endpointMemberListVersion;
    }

    public TriggerExplicitSuspicionOp() {
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl clusterService = getService();
        clusterService.triggerExplicitSuspicion(getCallerAddress(), callerMemberListVersion, endpoint, endpointMasterAddress, endpointMemberListVersion);
    }

    @Override
    public int getId() {
        return TRIGGER_EXPLICIT_SUSPICION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(callerMemberListVersion);
        out.writeObject(endpoint);
        out.writeObject(endpointMasterAddress);
        out.writeInt(endpointMemberListVersion);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        callerMemberListVersion = in.readInt();
        endpoint = in.readObject();
        endpointMasterAddress = in.readObject();
        endpointMemberListVersion = in.readInt();
    }

}
