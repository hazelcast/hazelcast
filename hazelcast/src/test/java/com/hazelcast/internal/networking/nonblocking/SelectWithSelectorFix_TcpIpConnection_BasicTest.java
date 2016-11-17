package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.nio.tcp.TcpIpConnection_BaseTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SelectWithSelectorFix_TcpIpConnection_BasicTest
        extends TcpIpConnection_BaseTest {

    @Before
    public void setup() throws Exception {
        threadingModelFactory = new SelectWithSelectorFix_NonBlockingIOThreadingModelFactory();
        super.setup();
    }
}
