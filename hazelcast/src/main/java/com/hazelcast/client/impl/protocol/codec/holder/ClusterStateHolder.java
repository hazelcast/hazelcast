package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.cluster.ClusterState;

public class ClusterStateHolder {
    private ClusterState state;

    public ClusterStateHolder(ClusterState state) {
        this.state = state;
    }

    public ClusterState getState() {
        return state;
    }

    public void setState(ClusterState state) {
        this.state = state;
    }
}
