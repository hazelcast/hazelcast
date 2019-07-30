package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.PredicateConfigHolder;
import com.hazelcast.nio.serialization.Data;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public class PredicateConfigHolderCodec {

    public static void encode(ClientMessage clientMessage, PredicateConfigHolder configHolder) {
        clientMessage.addFrame(BEGIN_FRAME);

        CodecUtil.encodeNullable(clientMessage, configHolder.getClassName(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, configHolder.getSql(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, configHolder.getImplementation(), DataCodec::encode);

        clientMessage.addFrame(END_FRAME);
    }

    public static PredicateConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        iterator.next(); // begin frame

        String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        String sql = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        Data implementation = CodecUtil.decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new PredicateConfigHolder(className, sql, implementation);
    }
}
