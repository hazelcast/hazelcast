package com.hazelcast.wm.test;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.web.SessionState;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Test;
import org.junit.Ignore;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class WebFilterSlowTests extends AbstractWebFilterTest {

    String testName;
    protected WebFilterSlowTests(String serverXml1) {
        super(serverXml1);
    }

    protected WebFilterSlowTests(String serverXml1, String serverXml2) {
        super(serverXml1, serverXml2);
        testName = serverXml1;
    }

    @Test
    public void test_github_issue_3360() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        //Creates session on server1
        executeRequest("write", serverPort1, cookieStore);
        //Reads value on server 1 (just to check that method works)
        assertEquals("value", executeRequest("readIfExist", serverPort1, cookieStore));
        //Reads value on server 2
        assertEquals("value", executeRequest("readIfExist", serverPort2, cookieStore));
    }

    @Test
    public void test_github_issue_2887() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);
        executeRequest("read", serverPort2, cookieStore);
        //expire session only on server2
        executeRequest("timeout", serverPort2, cookieStore);
        //Wait till session on server2 is expired
        sleepSeconds(2);
        //send redirect to server2 which has no local session but there is a distributed session.
        HttpResponse resp = request("redirect", serverPort2, cookieStore);
        assertEquals(302, resp.getStatusLine().getStatusCode());
    }

    @Test(timeout = 60000)
    public void testAttributeRemoval_issue_2618() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(1, map.size());
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("remove_set_null", serverPort2, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void test_update_server2_and_fetch_server1() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));

        assertEquals("value-updated", executeRequest("update-and-read-same-request", serverPort2, cookieStore));
        assertEquals("value-updated", executeRequest("update-and-read-same-request", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeNames_issue_2434() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
        //no name should be created
        assertEquals("", executeRequest("names", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void test_github_issue_2187() throws Exception {
        IMap<String, String> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(1, map.size());
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeDistribution() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(1, map.size());
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeRemoval() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(1, map.size());
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("remove", serverPort2, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeUpdate() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(1, map.size());
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("update", serverPort2, cookieStore));
        assertEquals("value-updated", executeRequest("read", serverPort1, cookieStore));
    }

    @Test
    public void whenClusterIsDown_enabledDeferredWrite() throws Exception {
        if (!testName.contains("deferred")) return;
        if (!testName.contains("client")) return;
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        hz.shutdown();
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("true", executeRequest("remove", serverPort1, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
        hz = Hazelcast.newHazelcastInstance(
                new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        Thread.sleep(8000);
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
    }

    @Test
    public void whenClusterIsDownAtBeginning_enabledDeferredWrite() throws Exception {
        if (!testName.contains("deferred")) return;
        if (!testName.contains("client")) return;
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
        Thread.sleep(8000);
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("true", executeRequest("remove", serverPort1, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
    }

    @Test
    @Ignore
    public void whenClusterIsDownAtBeginning_MapSizeAfterClusterIsUp() throws Exception {
        if (!testName.contains("deferred")) return;
        if (!testName.contains("client")) return;
        Hazelcast.shutdownAll();
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));

        hz = Hazelcast.newHazelcastInstance(
                new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        Thread.sleep(8000);
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        assertEquals(1, map.size());
    }

    @Test(timeout = 60000)
    public void testAttributeInvalidate() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(1, map.size());
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("invalidate", serverPort2, cookieStore));
        assertTrue(map.isEmpty());
    }

    @Test(timeout = 60000)
    public void testAttributeReloadSession() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(1, map.size());
        String oldSessionId = findHazelcastSessionId(map);
        assertNotNull(oldSessionId);
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("reload", serverPort2, cookieStore));
        String newSessionId = findHazelcastSessionId(map);
        assertEquals(1, map.size());
        assertNotNull(map.get(newSessionId));
        SessionState sessionState = (SessionState) map.get(newSessionId);
        SerializationService ss = getNode(hz).getSerializationService();
        assertEquals("first-value", ss.toObject(sessionState.getAttributes().get("first-key")));
        assertEquals("second-value", ss.toObject(sessionState.getAttributes().get("second-key")));
    }

    @Test
    public void testUpdateAndReadSameRequest() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals("value-updated", executeRequest("update-and-read-same-request", serverPort2, cookieStore));
    }

    @Test
    public void testUpdateAndReadSameRequestWithRestart() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        server1.restart();
        assertEquals("value-updated", executeRequest("update-and-read-same-request", serverPort1, cookieStore));
    }

    @Test
    public void testIssue3132() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("isNew", serverPort1, cookieStore));
        assertEquals("false", executeRequest("isNew", serverPort1, cookieStore));
        assertEquals("false", executeRequest("isNew", serverPort2, cookieStore));
        server1.restart();
        assertEquals("false", executeRequest("isNew", serverPort1, cookieStore));
        assertEquals("false", executeRequest("isNew", serverPort2, cookieStore));
    }


}
