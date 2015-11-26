package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.nio.tcp.TcpIpConnectionManager_ConnectOldClientBaseTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SelectNow_TcpIpConnectionManager_ConnectOldClientTest extends TcpIpConnectionManager_ConnectOldClientBaseTest {

    @Before
    public void setup() throws Exception {
        threadingModelFactory = new SelectNow_NonBlockingIOThreadingModelFactory();
        super.setup();
    }
}
