/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.wm.test;

import com.hazelcast.core.IMap;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(WebTestRunner.class)
@DelegatedRunWith(Parameterized.class)
@Category(QuickTest.class)
public class JettyWebFilterTest extends AbstractWebFilterTest {

    @Parameters(name = "Executing: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{"node - not deferred", "node1-node.xml", "node2-node.xml"}, //
                new Object[]{"node - deferred", "node1-node-deferred.xml", "node2-node-deferred.xml"}, //
                new Object[]{"client - not deferred", "node1-client.xml", "node2-client.xml"}, //
                new Object[]{"client - deferred", "node1-client-deferred.xml", "node2-client-deferred.xml"} //
        );
    }

    public JettyWebFilterTest(String name, String serverXml1, String serverXml2) {
        super(serverXml1, serverXml2);
    }

    @Test(timeout = 60000)
    public void testAttributeRemoval_issue_2618() throws Exception {
        IMap<String, Object> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(2, map.size());

        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("remove_set_null", serverPort2, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
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
        IMap<String, String> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(2, map.size());

        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeDistribution() throws Exception {
        IMap<String, Object> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(2, map.size());

        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeRemoval() throws Exception {
        IMap<String, Object> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(2, map.size());

        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("remove", serverPort2, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeUpdate() throws Exception {
        IMap<String, Object> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(2, map.size());

        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("update", serverPort2, cookieStore));
        assertEquals("value-updated", executeRequest("read", serverPort1, cookieStore));
    }

    @Test(timeout = 60000)
    public void testAttributeInvalidate() throws Exception {
        IMap<String, Object> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(2, map.size());

        assertEquals("value", executeRequest("read", serverPort2, cookieStore));

        assertEquals("true", executeRequest("invalidate", serverPort2, cookieStore));
        assertTrue(map.isEmpty());
    }

    @Test(timeout = 60000)
    public void testAttributeReloadSession() throws Exception {
        IMap<String, Object> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        assertEquals(2, map.size());

        String oldSessionId = findHazelcastSessionId(map);
        assertNotNull(oldSessionId);

        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals("true", executeRequest("reload", serverPort2, cookieStore));

        String newSessionId = findHazelcastSessionId(map);
        assertNotEquals("The old and new session IDs should not match", oldSessionId, newSessionId);
        assertEquals(3, map.size());
        assertEquals(1, map.get(newSessionId));
        assertEquals("first-value", map.get(newSessionId + HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR + "first-key"));
        assertEquals("second-value", map.get(newSessionId + HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR + "second-key"));

        assertNotEquals(oldSessionId, newSessionId);
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

        server1.stop();
        server1.start();

        assertEquals("value-updated", executeRequest("update-and-read-same-request", serverPort1, cookieStore));
    }

    @Test
    public void testIssue3132() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("true", executeRequest("isNew", serverPort1, cookieStore));
        assertEquals("false", executeRequest("isNew", serverPort1, cookieStore));
        assertEquals("false", executeRequest("isNew", serverPort2, cookieStore));
        server1.stop();
        server1.start();

        assertEquals("false", executeRequest("isNew", serverPort1, cookieStore));
        assertEquals("false", executeRequest("isNew", serverPort2, cookieStore));
    }

    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception{
        return new JettyServer(port,sourceDir,serverXml);
    }
}
