package com.hazelcast.client.connection;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.hazelfast.BlockingChannel;

public final class ClientBlockingChannel extends BlockingChannel {

    public ClientBlockingChannel(Context context) {
        super(context);
    }

    public void write(ClientMessage message) {
        byte[] data = message.buffer().byteArray();
        putInt(data.length);
        put(data);
    }

    // todo: deserialization of the client message should be done here.
}
