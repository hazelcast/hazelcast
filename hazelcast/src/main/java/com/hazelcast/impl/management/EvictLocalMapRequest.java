package com.hazelcast.impl.management;

import com.hazelcast.impl.EvictLocalMapEntriesCallable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EvictLocalMapRequest implements ConsoleRequest {

    String map;
    int percent;

    public EvictLocalMapRequest() {
    }

    public EvictLocalMapRequest(String map, int percent) {
        super();
        this.map = map;
        this.percent = percent;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EVICT_LOCAL_MAP;
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        EvictLocalMapEntriesCallable call = new EvictLocalMapEntriesCallable(map, percent);
        call.setHazelcastInstance(mcs.getHazelcastInstance());
        mcs.callOnAllMembers(call);
    }

    public Object readResponse(DataInput in) throws IOException {
        return null;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(map);
        out.writeInt(percent);
    }

    public void readData(DataInput in) throws IOException {
        map = in.readUTF();
        percent = in.readInt();
    }
}