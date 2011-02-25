package com.hazelcast.impl.management;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;

public interface  ConsoleRequest extends DataSerializable {

    int getType();

    Object readResponse(DataInputStream in) throws IOException;

    void writeResponse(ManagementConsoleService mcs, DataOutputStream dos) throws Exception;
}