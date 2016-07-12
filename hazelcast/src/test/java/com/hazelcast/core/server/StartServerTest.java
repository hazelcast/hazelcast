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

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class StartServerTest extends HazelcastTestSupport {

    private File parent = new File("ports");
    private File child = new File(parent, "hz.ports");

    @Before
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void setUp() {
        parent.mkdir();
    }

    @After
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void tearDown() {
        child.delete();
        parent.delete();

        Hazelcast.shutdownAll();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(StartServer.class);
    }

    @Test
    public void testMain() throws Exception {
        System.setProperty("print.port", child.getName());

        StartServer.main(new String[]{});

        assertEquals(1, Hazelcast.getAllHazelcastInstances().size());
        assertTrue(child.exists());
    }
}
