package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;

@GenerateParameters(id=2, name="Queue", ns = "Hazelcast.Client.Protocol.Queue")
public interface QueueTemplate {

    @EncodeMethod(id=1)
    void offer(String name, byte[] data, long timeoutMillis);

    @EncodeMethod(id=2)
    void poll(String name, long timeoutMillis);


}
