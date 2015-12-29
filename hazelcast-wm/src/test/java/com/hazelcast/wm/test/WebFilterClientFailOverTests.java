package com.hazelcast.wm.test;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public abstract class WebFilterClientFailOverTests extends AbstractWebFilterTest {


    @Parameterized.Parameters(name = "Executing: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{"client - not deferred", "node1-client.xml", "node2-client.xml"}, //
                new Object[]{"client - deferred", "node1-client-deferred.xml", "node2-client-deferred.xml"} //
        );
    }

    private String testName;

    protected WebFilterClientFailOverTests(String serverXml1) {
        super(serverXml1);
    }

    protected WebFilterClientFailOverTests(String serverXml1, String serverXml2) {
        super(serverXml1, serverXml2);
        testName = serverXml1;
    }

    @After
    public void destroy() {
        hz.shutdown();
    }

    @Test
    public void whenClusterIsDown_enabledDeferredWrite() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        hz.shutdown();

        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("true", executeRequest("remove", serverPort1, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));

        hz = Hazelcast.newHazelcastInstance(
                new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        assertClusterSizeEventually(1, hz);

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
    }

    @Test
    public void whenClusterIsDownAtBeginning_enabledDeferredWrite() throws Exception {
        hz.shutdown();

        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));

        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("true", executeRequest("remove", serverPort1, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));

        hz = Hazelcast.newHazelcastInstance(
                new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        assertClusterSizeEventually(1, hz);

        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("true", executeRequest("remove", serverPort1, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));

    }

    @Test
    public void testWhenClusterIsDownAtBeginningInNonDeferredMode() throws Exception {
        if (!testName.equals("client - not deferred")) {
            return;
        }

        hz.shutdown();
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));

        hz = Hazelcast.newHazelcastInstance(
                new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        assertClusterSizeEventually(1, hz);

        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        assertEquals(1, map.size());
    }

    @Test
    public void testWhenClusterIsDownAtBeginningInDeferedMode() throws Exception {
        if (!testName.equals("client - not deferred")) {
            return;
        }

        hz.shutdown();
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));

        hz = Hazelcast.newHazelcastInstance(
                new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        assertClusterSizeEventually(1, hz);
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));

        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        assertEquals(0, map.size());
    }
}
