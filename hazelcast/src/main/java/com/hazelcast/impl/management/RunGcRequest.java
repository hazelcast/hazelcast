package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: msk
 * Date: 12.12.2012
 * Time: 12:44
 */
public class RunGcRequest implements ConsoleRequest {

    public RunGcRequest(){
        super();
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_RUN_GC;
    }

    public Object readResponse(DataInput in) throws IOException {
        return "Successfully garbage collected.";
    }

    public void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception {
        System.gc();
    }

    public void writeData(DataOutput out) throws IOException {

    }

    public void readData(DataInput in) throws IOException {

    }
}
