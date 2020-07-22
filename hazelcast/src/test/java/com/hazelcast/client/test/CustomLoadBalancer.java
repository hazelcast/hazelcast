package com.hazelcast.client.test;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;

public class CustomLoadBalancer implements LoadBalancer {

    private static final String DEFAULT_NAME = "default-name";

    private String name;

    public CustomLoadBalancer() {
        this.name = DEFAULT_NAME;
    }

    @Override
    public void init(Cluster cluster, ClientConfig config) {
    }

    @Override
    public Member next() {
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
