package com.hazelcast.client.impl.protocol;

import com.hazelcast.instance.Node;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.client.impl.protocol.task.MessageTask;

public class JetMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];
    private final Node node;

    public JetMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    public void initFactories() {
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.jet
        factories[com.hazelcast.client.impl.protocol.codec.JetInterruptCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetInterruptMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetAcceptLocalizationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetAcceptLocalizationMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetInitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetInitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetGetAccumulatorsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetGetAccumulatorsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetExecuteCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetExecuteMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetSubmitCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetSubmitMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetLocalizeCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetLocalizeMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetEventCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetEventMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.JetFinalizeApplicationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.jet.JetFinalizeApplicationMessageTask(clientMessage, node, connection);
            }
        };
//endregion
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddPartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemovePartitionLostListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.CreateProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetDistributedObjectsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientDestroyProxyCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.DestroyProxyMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientPingCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.PingMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAddMembershipListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AddMembershipListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveAllListenersCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveAllListenersMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask(clientMessage, node, connection);
            }
        };
//endregion​
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    @Override
    public MessageTaskFactory[] getFactories() {
        return factories;
    }
}
