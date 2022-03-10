package com.hazelcast.alto.engine.nio;

import com.hazelcast.internal.tpc.nio.NioAsyncSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.alto.engine.AsyncSocketTest;
import com.hazelcast.internal.tpc.Eventloop;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class NioAsyncSocketTest extends AsyncSocketTest {

    @Override
    public Eventloop createEventloop() {
        return new NioEventloop();
    }

    @Override
    public AsyncSocket createAsyncSocket() {
        AsyncSocket socket =  NioAsyncSocket.open();
        closeables.add(socket);
        return socket;
    }
}
