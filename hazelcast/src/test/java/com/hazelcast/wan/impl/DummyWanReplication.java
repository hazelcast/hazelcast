package com.hazelcast.wan.impl;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEndpoint;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

class DummyWanReplication implements WanReplicationEndpoint {

    Queue<WanReplicationEvent> eventQueue = new ConcurrentLinkedQueue<WanReplicationEvent>();

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        WanReplicationEvent replicationEvent = new WanReplicationEvent(serviceName, eventObject);
        eventQueue.add(replicationEvent);
    }

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
    }

    @Override
    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        publishReplicationEvent(wanReplicationEvent.getServiceName(), wanReplicationEvent.getEventObject());
    }

    @Override
    public void checkWanReplicationQueues() {
    }

    Queue<WanReplicationEvent> getEventQueue() {
        return eventQueue;
    }
}
