package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.ForcedSyncResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

/**
 * Responsible for handling responses.
 */
final class ResponsePacketHandlerImpl implements ResponsePacketHandler {

    private OperationServiceImpl operationService;

    public ResponsePacketHandlerImpl(OperationServiceImpl operationService) {
        this.operationService = operationService;
    }

    @Override
    public Response deserialize(Packet packet) throws Exception {
        Data data = packet.getData();
        return (Response) operationService.nodeEngine.toObject(data);
    }

    @Override
    public void process(Response response) throws Exception {
        try {
            if (response instanceof NormalResponse || response instanceof CallTimeoutResponse) {
                notifyRemoteCall(response);
            } else if (response instanceof BackupResponse) {
                operationService.notifyBackupCall(response.getCallId());
            } else if (response instanceof ForcedSyncResponse) {
                notifyBackPressure(response.getCallId());
            } else {
                throw new IllegalStateException("Unrecognized response type: " + response);
            }
        } catch (Throwable e) {
            operationService.logger.severe("While processing response...", e);
        }
    }

    private void notifyBackPressure(long callId) {
        Invocation invocation = operationService.invocations.get(callId);
        if (invocation == null) {
            if (operationService.nodeEngine.isActive()) {
                throw new HazelcastException("No invocation for response: " + callId);
            }
            return;
        }

        invocation.notify(Invocation.BACKPRESSURE_RESPONSE);
    }

    // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
    private void notifyRemoteCall(Response response) {
        Invocation invocation = operationService.invocations.get(response.getCallId());
        if (invocation == null) {
            if (operationService.nodeEngine.isActive()) {
                throw new HazelcastException("No invocation for response: " + response);
            }
            return;
        }

        invocation.notify(response);
    }
}
