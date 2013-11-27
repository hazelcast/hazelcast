package com.hazelcast.client.clientMembershipTests;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * User: danny Date: 11/27/13
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MembershipTest {

    static HazelcastInstance client;
    static HazelcastInstance server;
    static CountMemberListener listener;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient(null);
        listener = new CountMemberListener();

        assertEquals(0, listener.getRunningCount());

        client.getCluster().addMembershipListener(listener);
    }

    @AfterClass
    public static void destroy() {
        client.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void MembershipListnerTest() throws Exception {

        HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        assertEquals(1, listener.getRunningCount());
        server2.shutdown();
        assertEquals(0, listener.getRunningCount());
    }
}
