package com.hazelcast.impl.management;

import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.readLongString;
import static com.hazelcast.nio.IOUtil.writeLongString;

public class ThreadDumpRequest implements ConsoleRequest {

    Address target;

    public ThreadDumpRequest() {
    }

    public ThreadDumpRequest(Address target) {
        this.target = target;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_THREAD_DUMP;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        String threadDump = (String) mcs.call(target, new ThreadDumpCallable());
        writeLongString(dos, threadDump);
    }

    public String readResponse(DataInput in) throws IOException {
        return readLongString(in);
    }

    public void writeData(DataOutput out) throws IOException {
        target.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        target = new Address();
        target.readData(in);
    }
}
