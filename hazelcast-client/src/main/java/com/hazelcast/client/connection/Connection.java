package com.hazelcast.client.connection;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.io.IOException;

/**
 * @mdogan 5/15/13
 */
public interface Connection extends Closeable {

    Address getEndpoint();

    boolean write(Data data) throws IOException;

    Data read() throws IOException;

    int getId();

    long getLastReadTime();

    void close() throws IOException;

}
