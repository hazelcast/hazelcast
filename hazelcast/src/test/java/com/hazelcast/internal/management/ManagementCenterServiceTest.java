package com.hazelcast.internal.management;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.management.ManagementCenterService.cleanupUrl;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ManagementCenterServiceTest extends HazelcastTestSupport {

    @Before
    public void setUp() throws Exception {
        setLoggingLog4j();
        setLogLevel(Level.DEBUG);
    }

    @After
    public void tearDown() throws Exception {
        resetLogLevel();

        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testConstructor_withNullConfiguration() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        instance.getConfig().setManagementCenterConfig(null);
        new ManagementCenterService(getNode(instance).hazelcastInstance);
    }

    @Test
    public void testCleanupUrl() {
        String url = cleanupUrl("http://noCleanupNeeded/");
        assertTrue(url.endsWith("/"));
    }

    @Test
    public void testCleanupUrl_needsCleanup() {
        String url = cleanupUrl("http://needsCleanUp");
        assertTrue(url.endsWith("/"));
    }

    @Test
    public void testCleanupUrl_withNull() {
        assertNull(cleanupUrl(null));
    }
}
