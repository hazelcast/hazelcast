/**
 * 
 */
package com.hazelcast.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.Connection;

public abstract class AbstractRemotelyProcessable implements RemotelyProcessable {
    Connection conn;

    public Connection getConnection() {
        return conn;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public void readData(DataInput in) throws IOException {
    }

    public void writeData(DataOutput out) throws IOException {
    }
}