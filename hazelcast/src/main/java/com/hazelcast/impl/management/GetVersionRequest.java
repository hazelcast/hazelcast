package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 12.12.2012
 * Time: 11:20
 */
public class GetVersionRequest implements ConsoleRequest{

    public GetVersionRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_VERSION;
    }

    public Object readResponse(DataInput in) throws IOException {
        return in.readUTF();
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        dos.writeUTF(mcs.getHazelcastInstance().node.initializer.getVersion());
    }

    public void writeData(DataOutput out) throws IOException {

    }

    public void readData(DataInput in) throws IOException {

    }
}
