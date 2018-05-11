package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AtomicLongMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    public static final int CREATE_TYPE = 10000;
    public static final int ADD_AND_GET_TYPE = 10001;
    public static final int GET_AND_ADD_TYPE = 10002;
    public static final int GET_AND_SET_TYPE = 10003;
    public static final int COMPARE_AND_SET_TYPE = 10004;
    public static final int DESTROY_TYPE = 10005;

    private final Node node;

    public AtomicLongMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

        factories[CREATE_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CreateAtomicLongMessageTask(clientMessage, node, connection);
            }
        };

        factories[ADD_AND_GET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new AddAndGetMessageTask(clientMessage, node, connection);
            }
        };

        factories[GET_AND_ADD_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetAndAddMessageTask(clientMessage, node, connection);
            }
        };

        factories[GET_AND_SET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetAndSetMessageTask(clientMessage, node, connection);
            }
        };

        factories[COMPARE_AND_SET_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CompareAndSetMessageTask(clientMessage, node, connection);
            }
        };
        factories[DESTROY_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new DestroyAtomicLongMessageTask(clientMessage, node, connection);
            }
        };

        return factories;
    }
}
