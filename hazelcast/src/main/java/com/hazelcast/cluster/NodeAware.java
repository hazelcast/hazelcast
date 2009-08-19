package com.hazelcast.cluster;

import com.hazelcast.impl.Node;

/**
 * Created by IntelliJ IDEA.
 * User: talipozturk
 * Date: Aug 19, 2009
 * Time: 2:19:07 AM
 * To change this template use File | Settings | File Templates.
 */
public interface NodeAware {
    Node getNode();

    void setNode(Node node);
}
