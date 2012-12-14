package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 12.12.2012
 * Time: 10:18
 */
public class GetLogLevelRequest implements ConsoleRequest {

    public GetLogLevelRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOG_LEVEL;
    }

    public Object readResponse(DataInput in) throws IOException {
        return in.readUTF();
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        dos.writeUTF(mcs.getHazelcastInstance().node.getSystemLogService().getCurrentLevel());
    }

    public void writeData(DataOutput out) throws IOException {

    }

    public void readData(DataInput in) throws IOException {

    }
}
