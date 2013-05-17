package com.hazelcast.client.connection;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

/**
 * @mdogan 5/15/13
 */
class Router {

    private final LoadBalancer loadBalancer;

    Router(LoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    public Address next() {
        final MemberImpl member = (MemberImpl) loadBalancer.next();
        return member.getAddress();
    }
}
