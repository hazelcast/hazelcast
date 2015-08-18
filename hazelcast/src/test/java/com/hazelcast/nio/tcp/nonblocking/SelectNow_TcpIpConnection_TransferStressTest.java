package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.nio.tcp.TcpIpConnection_TransferStressTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SelectNow_TcpIpConnection_TransferStressTest extends TcpIpConnection_TransferStressTest {

    @Before
    public void setup() throws Exception {
        threadingModelFactory = new SelectNow_NonBlockingTcpIpConnectionThreadingModelFactory();
        super.setup();
    }
}
