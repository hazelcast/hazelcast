package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;

public interface  ConsoleRequest extends DataSerializable {

    int getType();

    Object readResponse(DataInput in) throws IOException;

    void writeResponse(ManagementConsoleService mcs, DataOutput dos) throws Exception;
}