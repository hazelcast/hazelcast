package com.hazelcast.cluster;

import com.hazelcast.impl.Node;

/**
 * Created by IntelliJ IDEA.
 * User: talipozturk
 * Date: Aug 19, 2009
 * Time: 2:20:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class AbstractNodeAware {
    volatile Node node;

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }
}
