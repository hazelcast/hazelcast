package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.nio.tcp.TcpIpConnectionManager_ConnectMemberBaseTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class SelectNow_TcpIpConnectionManager_ConnectMemberTest extends TcpIpConnectionManager_ConnectMemberBaseTest {

    @Before
    public void setup() throws Exception {
        threadingModelFactory = new SelectNow_NonBlockingIOThreadingModelFactory();
        super.setup();
    }
}
