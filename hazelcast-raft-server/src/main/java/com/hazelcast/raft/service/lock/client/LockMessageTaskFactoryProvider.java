package com.hazelcast.raft.service.lock.client;

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
public class LockMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    public static final int CREATE_TYPE = 20000;
    public static final int LOCK = 20001;
    public static final int UNLOCK = 20002;
    public static final int LOCK_COUNT = 20003;
    public static final int TRY_LOCK = 20004;

    private final Node node;

    public LockMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

        factories[CREATE_TYPE] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new CreateLockMessageTask(clientMessage, node, connection);
            }
        };

        factories[LOCK] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new LockMessageTask(clientMessage, node, connection);
            }
        };

        factories[UNLOCK] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new UnlockMessageTask(clientMessage, node, connection);
            }
        };

        factories[LOCK_COUNT] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new GetLockCountMessageTask(clientMessage, node, connection);
            }
        };

        factories[TRY_LOCK] = new MessageTaskFactory() {
            @Override
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new TryLockMessageTask(clientMessage, node, connection);
            }
        };
        return factories;
    }
}
