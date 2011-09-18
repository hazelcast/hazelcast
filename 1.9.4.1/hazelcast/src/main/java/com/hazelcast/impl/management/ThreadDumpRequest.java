package com.hazelcast.impl.management;

import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.readLongString;
import static com.hazelcast.nio.IOUtil.writeLongString;

public class ThreadDumpRequest implements ConsoleRequest {

	private boolean isDeadlock;
    private Address target;

    public ThreadDumpRequest() {
    }

    public ThreadDumpRequest(Address target, boolean deadlock) {
        this.target = target;
        this.isDeadlock = deadlock;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_THREAD_DUMP;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        String threadDump = (String) mcs.call(target, new ThreadDumpCallable(isDeadlock));
        if(threadDump != null) {
        	dos.writeBoolean(true);
        	writeLongString(dos, threadDump);
        }
        else {
        	dos.writeBoolean(false);
        }
    }

    public String readResponse(DataInput in) throws IOException {
    	if(in.readBoolean()) {
    		return readLongString(in);
    	}
    	else {
    		return null;
    	}
    }

    public void writeData(DataOutput out) throws IOException {
        target.writeData(out);
        out.writeBoolean(isDeadlock);
    }

    public void readData(DataInput in) throws IOException {
        target = new Address();
        target.readData(in);
        isDeadlock = in.readBoolean();
    }
}
