package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.protocol.map.MapMessageType;
import com.hazelcast.client.impl.protocol.map.MapPutMessageTask;
import com.hazelcast.client.impl.protocol.util.Int2ObjectHashMap;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

/**
 * Message task factory
 */
public class MessageTaskFactoryImpl {

    private final Int2ObjectHashMap<MessageTaskFactory> tasks = new Int2ObjectHashMap<MessageTaskFactory>();

    private final Node node;

    public MessageTaskFactoryImpl(Node node) {
        this.node = node;
        initFactories();
    }

    public void initFactories() {
        tasks.put(ClientMessageType.AUTHENTICATION_DEFAULT.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new AuthenticationMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(ClientMessageType.AUTHENTICATION_CUSTOM.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        });
        tasks.put(MapMessageType.MAP_PUT.id(), new MessageTaskFactory() {
            public PartitionSpecificRunnable create(ClientMessage clientMessage, Node node, Connection connection) {
                return new MapPutMessageTask(clientMessage, node, connection);
            }
        });

        //TODO more factories to come here
    }

    public PartitionSpecificRunnable createMessageTask(ClientMessage clientMessage, Connection connection) {
        final MessageTaskFactory factory = tasks.get(clientMessage.getMessageType());
        if (factory != null) {
            return factory.create(clientMessage, node, connection);
        }
        return new NoSuchMessageTask(clientMessage, node, connection);
    }

}
