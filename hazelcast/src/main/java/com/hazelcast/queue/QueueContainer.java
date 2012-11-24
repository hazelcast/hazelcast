package com.hazelcast.queue;

import com.hazelcast.nio.Data;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/22/12
 * Time: 11:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueueContainer {

    protected Queue<Data> dataQueue = new LinkedList<Data>();

    protected final int partitionId;

    public QueueContainer(int partitionId) {
        this.partitionId = partitionId;
    }


}
