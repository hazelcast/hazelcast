/**
 * 
 */
package com.hazelcast.cluster;

import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.DataSerializable;

public interface RemotelyProcessable extends DataSerializable, Processable, NodeAware {
    void setConnection(Connection conn);
}