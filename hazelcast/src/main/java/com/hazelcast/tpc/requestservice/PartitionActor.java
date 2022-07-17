package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.actor.Actor;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;


/**
 * Processes messages within a partition. Issues like replication already have
 * been taken care of by the infrastructure.
 */
public class PartitionActor extends Actor {

    @Override
    public void process(Object m) {
        IOBuffer request = (IOBuffer) m;
    }
}
