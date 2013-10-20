/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.hazelcast.core.IMap;

/**
 * @author mdogan 5/11/12
 */
@RunWith(Parameterized.class)
public class WebfilterTestCase extends AbstractWebfilterTestCase {

    @Parameters(name = "Executing: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { new Object[] { "node - not cached", "node1-node.xml", "node2-node.xml" },
                new Object[] { "node - cached", "node1-node-cached.xml", "node2-node-cached.xml" }, new Object[] { "client - not cached", "node1-client.xml", "node2-client.xml" },
                new Object[] { "client - cached", "node1-client-cached.xml", "node2-client-cached.xml" } });
    }

    public WebfilterTestCase(String name, String serverXml1, String serverXml2) {
        this.serverXml1 = serverXml1;
        this.serverXml2 = serverXml2;
    }

    @Test(timeout = 60000)
    public void testAttributeDistribution() throws Exception {
        IMap<String, Object> map = hz.getMap("default");

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        Set<Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(2, entrySet.size());

        String value = executeRequest("read", serverPort2, cookieStore);
        assertEquals("value", value);
    }

    @Test(timeout = 60000)
    public void testAttributeRemoval() throws Exception {
        IMap<String, Object> map = hz.getMap("default");

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        Set<Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(2, entrySet.size());

        String value = executeRequest("read", serverPort2, cookieStore);
        assertEquals("value", value);

        value = executeRequest("remove", serverPort2, cookieStore);
        assertEquals("true", value);

        value = executeRequest("read", serverPort1, cookieStore);
        assertEquals("null", value);
    }

    @Test(timeout = 60000)
    public void testAttributeUpdate() throws Exception {
        IMap<String, Object> map = hz.getMap("default");

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        Set<Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(2, entrySet.size());

        String value = executeRequest("read", serverPort2, cookieStore);
        assertEquals("value", value);

        value = executeRequest("update", serverPort2, cookieStore);
        assertEquals("true", value);

        value = executeRequest("read", serverPort1, cookieStore);
        assertEquals("value-updated", value);
    }

    @Test(timeout = 60000)
    public void testAttributeInvalidate() throws Exception {
        IMap<String, Object> map = hz.getMap("default");

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        Set<Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(2, entrySet.size());

        String value = executeRequest("read", serverPort2, cookieStore);
        assertEquals("value", value);

        value = executeRequest("invalidate", serverPort2, cookieStore);
        assertEquals("true", value);

        entrySet = map.entrySet();
        assertEquals(0, entrySet.size());
    }

    @Test(timeout = 60000)
    public void testAttributeReloadSession() throws Exception {
        IMap<String, Object> map = hz.getMap("default");

        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        Set<Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(2, entrySet.size());

        String oldSessionId = findHazelcastSessionId(map);

        assertNotNull(oldSessionId);

        String value = executeRequest("read", serverPort2, cookieStore);
        assertEquals("value", value);

        value = executeRequest("reload", serverPort2, cookieStore);
        assertEquals("true", value);

        String newSessionId = findHazelcastSessionId(map);

        entrySet = map.entrySet();
        assertEquals(3, entrySet.size());
        assertEquals(Boolean.TRUE, map.get(newSessionId));
        assertEquals("first-value", map.get(newSessionId + HAZELCAST_SESSION_ATTRIBUTE_SEPERATOR + "first-key"));
        assertEquals("second-value", map.get(newSessionId + HAZELCAST_SESSION_ATTRIBUTE_SEPERATOR + "second-key"));

        assertNotEquals(oldSessionId, newSessionId);
    }

}
