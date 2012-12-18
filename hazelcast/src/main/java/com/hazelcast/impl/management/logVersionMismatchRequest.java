package com.hazelcast.impl.management;

import com.hazelcast.logging.ILogger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 17.12.2012
 * Time: 12:32
 */
public class logVersionMismatchRequest implements ConsoleRequest {

    private String manCenterVersion;

    public logVersionMismatchRequest(String manCenterVersion){
        this.manCenterVersion = manCenterVersion;
    }

    public logVersionMismatchRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOG_VERSION_MISMATCH;
    }

    public Object readResponse(DataInput in) throws IOException {
        return "SUCCESS";
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        final ILogger logger = mcs.getHazelcastInstance().node.getLogger(logVersionMismatchRequest.class.getName());
        logger.log(Level.SEVERE, "The version of the management center is " + manCenterVersion);
    }

    public void writeData(DataOutput out) throws IOException {
       out.writeUTF(manCenterVersion);
    }

    public void readData(DataInput in) throws IOException {
       manCenterVersion = in.readUTF();
    }
}
