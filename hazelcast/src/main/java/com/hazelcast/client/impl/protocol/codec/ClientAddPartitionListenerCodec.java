package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;

import javax.annotation.Generated;

@Generated("Hazelcast.code.generator")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class ClientAddPartitionListenerCodec {

    public static final int REQUEST_TYPE = 0x0012;
    public static final int RESPONSE_TYPE = 104;
    public static final boolean RETRYABLE = false;

    //************************ REQUEST *************************//

    public static class RequestParameters {
        public static final int TYPE = REQUEST_TYPE;

        public static int calculateDataSize() {
            return ClientMessage.HEADER_SIZE;
        }
    }

    public static ClientMessage encodeRequest() {
        final int requiredDataSize = RequestParameters.calculateDataSize();
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE);
        clientMessage.setRetryable(RETRYABLE);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        return new RequestParameters();
    }

    //************************ RESPONSE *************************//
    public static ClientMessage encodeResponse() {
        int requiredDataSize = MapSetCodec.ResponseParameters.calculateDataSize();
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(100);
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    //************************ EVENTS *************************//

    public static ClientMessage encodePartitionsEvent(java.util.Collection<java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>>> partitions) {
        int dataSize = ClientMessage.HEADER_SIZE;
        dataSize += Bits.INT_SIZE_IN_BYTES;
        for (java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>> partitions_item : partitions) {
            com.hazelcast.nio.Address partitions_itemKey = partitions_item.getKey();
            java.util.List<java.lang.Integer> partitions_itemVal = partitions_item.getValue();
            dataSize += com.hazelcast.client.impl.protocol.codec.AddressCodec.calculateDataSize(partitions_itemKey);
            dataSize += Bits.INT_SIZE_IN_BYTES;
            for (java.lang.Integer partitions_itemVal_item : partitions_itemVal) {
                dataSize += ParameterUtil.calculateDataSize(partitions_itemVal_item);
            }
        }

        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(217);
        clientMessage.addFlag(ClientMessage.LISTENER_EVENT_FLAG);

        clientMessage.set(partitions.size());
        for (java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>> partitions_item : partitions) {
            com.hazelcast.nio.Address partitions_itemKey = partitions_item.getKey();
            java.util.List<java.lang.Integer> partitions_itemVal = partitions_item.getValue();
            com.hazelcast.client.impl.protocol.codec.AddressCodec.encode(partitions_itemKey, clientMessage);
            clientMessage.set(partitions_itemVal.size());
            for (java.lang.Integer partitions_itemVal_item : partitions_itemVal) {
                clientMessage.set(partitions_itemVal_item);
            }
        }
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    ;


    public static abstract class AbstractEventHandler {

        public void handle(ClientMessage clientMessage) {
            int messageType = clientMessage.getMessageType();
            if (messageType == 217) {
                boolean messageFinished = false;
                java.util.List<java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>>> partitions = null;
                if (!messageFinished) {
                    messageFinished = clientMessage.isComplete();
                }
                if (!messageFinished) {
                    int partitions_size = clientMessage.getInt();
                    partitions = new java.util.ArrayList<java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>>>(partitions_size);
                    for (int partitions_index = 0; partitions_index < partitions_size; partitions_index++) {
                        java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>> partitions_item;
                        com.hazelcast.nio.Address partitions_item_key;
                        java.util.List<java.lang.Integer> partitions_item_val;
                        partitions_item_key = com.hazelcast.client.impl.protocol.codec.AddressCodec.decode(clientMessage);
                        int partitions_item_val_size = clientMessage.getInt();
                        partitions_item_val = new java.util.ArrayList<java.lang.Integer>(partitions_item_val_size);
                        for (int partitions_item_val_index = 0; partitions_item_val_index < partitions_item_val_size; partitions_item_val_index++) {
                            java.lang.Integer partitions_item_val_item;
                            partitions_item_val_item = clientMessage.getInt();
                            partitions_item_val.add(partitions_item_val_item);
                        }
                        partitions_item = new java.util.AbstractMap.SimpleEntry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>>(partitions_item_key, partitions_item_val);
                        partitions.add(partitions_item);
                    }
                }
                handle(partitions);
                return;
            }
            com.hazelcast.logging.Logger.getLogger(super.getClass()).warning("Unknown message type received on event handler :" + clientMessage.getMessageType());
        }

        public abstract void handle(java.util.Collection<java.util.Map.Entry<com.hazelcast.nio.Address, java.util.List<java.lang.Integer>>> partitions);

    }

}
