package com.hazelcast.client.impl.protocol;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

/**
 * Message task factory interface
 */
public interface MessageTaskFactory {

    PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection);

}
