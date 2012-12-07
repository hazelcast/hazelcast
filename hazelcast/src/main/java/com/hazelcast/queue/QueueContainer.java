package com.hazelcast.queue;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.Data;

import java.util.LinkedList;
import java.util.Queue;

/**
 * User: ali
 * Date: 11/22/12
 * Time: 11:00 AM
 */
public class QueueContainer {

    final Queue<Data> dataQueue = new LinkedList<Data>();

    final int partitionId;

    QueueConfig config;

    public QueueContainer(int partitionId, QueueConfig config) {
        this.partitionId = partitionId;
        this.config = config;
    }


}
