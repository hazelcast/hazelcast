package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.nio.tcp.TcpIpConnectionManager_ConnectMemberTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class Spinning_TcpIpConnectionManager_ConnectMemberTest extends TcpIpConnectionManager_ConnectMemberTest {

    @Before
    public void setup() throws Exception {
        threadingModelFactory = new Spinning_IOThreadingModelFactory();
        super.setup();
    }
}
