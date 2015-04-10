package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.nio.Connection;

/**
 * Message task factory interface
 */
public interface MessageTaskFactory {

    MessageTask create(ClientMessage clientMessage, Connection connection);

}
