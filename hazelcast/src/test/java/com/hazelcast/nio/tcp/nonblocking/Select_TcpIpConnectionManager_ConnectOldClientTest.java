package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.nio.tcp.TcpIpConnectionManager_ConnectOldClientTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class Select_TcpIpConnectionManager_ConnectOldClientTest extends TcpIpConnectionManager_ConnectOldClientTest {

    @Before
    public void setup() throws Exception {
        threadingModelFactory = new Select_NonBlockingIOThreadingModelFactory();
        super.setup();
    }
}
