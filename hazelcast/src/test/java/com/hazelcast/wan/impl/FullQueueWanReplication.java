package com.hazelcast.wan.impl;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WANReplicationQueueFullException;
import com.hazelcast.wan.WanReplicationEndpoint;
import com.hazelcast.wan.WanReplicationEvent;

public class FullQueueWanReplication implements WanReplicationEndpoint {

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
    }

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
    }

    @Override
    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
    }

    @Override
    public void checkWanReplicationQueues() {
        throw new WANReplicationQueueFullException("WAN event queue is full");
    }
}
