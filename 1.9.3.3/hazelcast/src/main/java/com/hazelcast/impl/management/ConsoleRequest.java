package com.hazelcast.impl.management;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ConsoleRequest extends DataSerializable {

    int getType();

    Object readResponse(DataInput in) throws IOException;

    void writeResponse(ManagementCenterService mcs, DataOutput dos) throws Exception;
}