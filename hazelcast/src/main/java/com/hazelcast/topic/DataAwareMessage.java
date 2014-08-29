package com.hazelcast.topic;

import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * User: sancar
 * Date: 29/08/14
 * Time: 14:19
 */
public class DataAwareMessage extends Message {

    protected final Data messageData;
    private final transient SerializationService serializationService;

    public DataAwareMessage(String topicName, Data messageData, long publishTime, Member publishingMember, SerializationService serializationService) {
        super(topicName, null, publishTime, publishingMember);
        this.serializationService = serializationService;
        this.messageData = messageData;
    }

    public Object getMessageObject() {
        if (messageObject == null && messageData != null) {
            messageObject = serializationService.toObject(messageData);
        }
        return messageObject;
    }

    public Data getMessageData() {
        return messageData;
    }
}
