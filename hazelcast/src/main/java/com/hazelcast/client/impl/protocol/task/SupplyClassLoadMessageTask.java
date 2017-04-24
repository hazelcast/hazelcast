package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.ClientClassLoadDemandService;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientSupplyClassLoadCodec;
import com.hazelcast.client.impl.protocol.codec.ClientSupplyClassLoadCodec.RequestParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class SupplyClassLoadMessageTask extends AbstractCallableMessageTask<ClientSupplyClassLoadCodec.RequestParameters> {

    public SupplyClassLoadMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    protected Object call() throws Exception {
        ClientClassLoadDemandService service = nodeEngine.getService(ClientClassLoadDemandService.SERVICE_NAME);
        service.putClassData(parameters.className, parameters.classDefinition);
        return null;
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientSupplyClassLoadCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientSupplyClassLoadCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
