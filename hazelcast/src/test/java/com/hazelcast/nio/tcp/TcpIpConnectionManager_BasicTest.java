package com.hazelcast.nio.tcp;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpConnectionManager_BasicTest extends TcpIpConnection_AbstractTest {

    @Test
    public void start() {
        connManagerA.start();

        assertTrue(connManagerA.isLive());
    }

    @Test
    public void start_whenAlreadyStarted_thenCallIgnored() {
        //first time
        connManagerA.start();

        //second time
        connManagerA.start();

        assertTrue(connManagerA.isLive());
    }
}
