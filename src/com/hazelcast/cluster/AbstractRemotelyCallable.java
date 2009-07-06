/**
 * 
 */
package com.hazelcast.cluster;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

public abstract class AbstractRemotelyCallable<T> implements DataSerializable,
        Callable<T> {
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