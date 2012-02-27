package com.hazelcast.impl.management;

import com.hazelcast.impl.monitor.MemberStateImpl;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.monitor.TimedClusterState;
import com.hazelcast.monitor.TimedMemberState;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GetMemberStateRequest implements ConsoleRequest {

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_STATE;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        TimedMemberState ts = mcs.getTimedMemberState();
        ts.writeData(dos);
    }

    public TimedMemberState readResponse(DataInput in) throws IOException {
        TimedMemberState t = new TimedMemberState();
        t.readData(in);
        return t;
    }

    public void writeData(DataOutput out) throws IOException {
    }

    public void readData(DataInput in) throws IOException {
    }
}
