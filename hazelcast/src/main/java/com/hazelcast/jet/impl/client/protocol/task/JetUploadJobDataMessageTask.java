package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetUploadJobDataCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

public class JetUploadJobDataMessageTask extends
        AbstractJetMessageTask<JetUploadJobDataCodec.RequestParameters, Boolean> {

    protected JetUploadJobDataMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetUploadJobDataCodec::decodeRequest,
                JetUploadJobDataCodec::encodeResponse);
    }

    @Override
    protected Operation prepareOperation() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "uploadJobData";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
