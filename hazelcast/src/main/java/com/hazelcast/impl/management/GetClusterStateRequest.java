package com.hazelcast.impl.management;

import com.hazelcast.monitor.TimedClusterState;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GetClusterStateRequest implements ConsoleRequest {

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_STATE;
    }

    public void writeResponse(ManagementConsoleService mcs, DataOutput dos) throws Exception {
        mcs.writeState(dos);
    }

    public TimedClusterState readResponse(DataInput in) throws IOException {
        TimedClusterState t = new TimedClusterState();
        t.readData(in);
        return t;
    }

    public void writeData(DataOutput out) throws IOException {
    }

    public void readData(DataInput in) throws IOException {
    }
}