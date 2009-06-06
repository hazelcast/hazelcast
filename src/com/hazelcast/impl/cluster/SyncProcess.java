/**
 * 
 */
package com.hazelcast.impl.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.impl.BlockingQueueManager;
import com.hazelcast.impl.ConcurrentMapManager;
import com.hazelcast.impl.ListenerManager;
import com.hazelcast.impl.TopicManager;
import com.hazelcast.nio.Connection;

public class SyncProcess extends AbstractRemotelyCallable<Boolean> implements
        RemotelyProcessable {

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

    public Boolean call() {
        process();
        return Boolean.TRUE;
    }

    public void process() {
        ConcurrentMapManager.get().syncForAdd();
        BlockingQueueManager.get().syncForAdd();
        ListenerManager.get().syncForAdd();
        TopicManager.get().syncForAdd();
        ClusterManager.get().joinReset();
    }
}