package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCApplyMCConfigCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;

public class ApplyMCConfigMessageTask extends AbstractCallableMessageTask<MCApplyMCConfigCodec.RequestParameters> {

    public ApplyMCConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();
        ClientBwListDTO.Mode mode = ClientBwListDTO.Mode.getById(parameters.clientBwListMode);
        if (mode == null) {
            throw new IllegalArgumentException("Unexpected client B/W list mode = [" + parameters.clientBwListMode + "]");
        }
        mcService.applyConfig(parameters.eTag, new ClientBwListDTO(mode, parameters.clientBwListEntries));
        return null;
    }

    @Override
    protected MCApplyMCConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCApplyMCConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCApplyMCConfigCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "applyMCConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {
                parameters.eTag,
                parameters.clientBwListMode,
                parameters.clientBwListEntries
        };
    }
}
