package com.hazelcast.core.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class StartServerTest extends HazelcastTestSupport {

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(StartServer.class);
    }

    @Test
    public void testMain() {
        StartServer.main(new String[]{});

        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
    }
}
