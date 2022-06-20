package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.actor.Actor;
import com.hazelcast.tpc.engine.frame.Frame;


/**
 * Processes messages within a partition. Issues like replication already have
 * been taken care of by the infrastructure.
 */
public class PartitionActor extends Actor {

    @Override
    public void process(Object m) {
        Frame request = (Frame) m;
    }
}
