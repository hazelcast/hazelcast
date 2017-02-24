package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

// TODO [basri] ADD JAVADOC
public class ExplicitSuspicionOperation extends AbstractClusterOperation {


    private Address suspectedAddress;

    private boolean destroyConnection;

    public ExplicitSuspicionOperation(Address suspectedAddress, boolean destroyConnection) {
        this.suspectedAddress = suspectedAddress;
        this.destroyConnection = destroyConnection;
    }

    public ExplicitSuspicionOperation() {
    }

    @Override
    public void run() throws Exception {
        final ClusterServiceImpl clusterService = getService();
        clusterService.suspectAddress(suspectedAddress, "explicit suspicion", destroyConnection);
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(suspectedAddress);
        out.writeBoolean(destroyConnection);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        suspectedAddress = in.readObject();
        destroyConnection = in.readBoolean();
    }

}
