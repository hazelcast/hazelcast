package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetUploadJobMetaDataCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

public class JetUploadJobMetaDataMessageTask extends
        AbstractJetMessageTask<JetUploadJobMetaDataCodec.RequestParameters, Boolean> {

    protected JetUploadJobMetaDataMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetUploadJobMetaDataCodec::decodeRequest,
                JetUploadJobMetaDataCodec::encodeResponse);
    }

    @Override
    protected Operation prepareOperation() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "uploadJobMetaData";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
