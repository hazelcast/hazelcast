package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 12.12.2012
 * Time: 10:25
 */
public class SetLogLevelRequest implements ConsoleRequest {

    public String logLevel;

    public SetLogLevelRequest(){
        super();
    }

    public SetLogLevelRequest(String logLevel){
        this.logLevel = logLevel;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_SET_LOG_LEVEL;
    }

    public Object readResponse(DataInput in) throws IOException {
        return "success";
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        mcs.getHazelcastInstance().node.getSystemLogService().setCurrentLevel(logLevel);
    }

    public void writeData(DataOutput out) throws IOException {
       out.writeUTF(logLevel);
    }

    public void readData(DataInput in) throws IOException {
        logLevel = in.readUTF();
    }
}
