package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import com.hazelcast.nio.Address;

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

    public void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception {
        String threadDump = (String) mcs.call(target, new ThreadDumpCallable());
        dos.writeUTF(threadDump);
    }

    public String readResponse(DataInputStream in) throws IOException {
        return in.readUTF();
    }

    public void writeData(DataOutput out) throws IOException {
        target.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        target = new Address();
        target.readData(in);
    }
}
