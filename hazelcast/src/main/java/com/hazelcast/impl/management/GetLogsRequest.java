package com.hazelcast.impl.management;

import com.hazelcast.impl.base.SystemLogRecord;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 12.12.2012
 * Time: 10:37
 */
public class GetLogsRequest implements ConsoleRequest {

    public GetLogsRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOGS;
    }

    public Object readResponse(DataInput in) throws IOException {
        List<SystemLogRecord> list = new LinkedList<SystemLogRecord>();
        String node = in.readUTF();
        int size = in.readInt();
        for(int i = 0; i < size ; i++){
            SystemLogRecord systemLogRecord = new SystemLogRecord();
            systemLogRecord.readData(in);
            systemLogRecord.setNode(node);
            list.add(systemLogRecord);
        }
        return list;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        List<SystemLogRecord> logBundle = mcs.getHazelcastInstance().node.getSystemLogService().getLogBundle();
        final Address address = mcs.getHazelcastInstance().node.getThisAddress();
        dos.writeUTF(address.getHost() + ":" + address.getPort() );
        dos.writeInt(logBundle.size());
        for (SystemLogRecord systemLogRecord : logBundle) {
            systemLogRecord.writeData(dos);
        }
    }

    public void writeData(DataOutput out) throws IOException {

    }

    public void readData(DataInput in) throws IOException {

    }
}
