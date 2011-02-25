package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import com.hazelcast.monitor.TimedClusterState;

public class GetClusterStateRequest implements ConsoleRequest {
   
	public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_STATE;
    }

    public void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception {
        mcs.writeState(dos);
    }

    public TimedClusterState readResponse(DataInputStream in) throws IOException {
        TimedClusterState t = new TimedClusterState();
        t.readData(in);
        return t;
    }

    public void writeData(DataOutput out) throws IOException {
    }

    public void readData(DataInput in) throws IOException {
    }
}