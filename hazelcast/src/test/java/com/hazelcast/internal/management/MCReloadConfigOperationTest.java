package com.hazelcast.internal.management;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.codec.MCReloadConfigCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.map.AbstractClientMapTest;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class MCReloadConfigOperationTest
        extends AbstractClientMapTest {

    @Test
    public void test()
            throws ExecutionException, InterruptedException {
        ClientInvocation inv = new ClientInvocation(
                ((HazelcastClientProxy) client).client,
                MCReloadConfigCodec.encodeRequest(),
                null
        );
        inv.invoke().get();
    }
}
