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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests to basic session methods. getAttribute,setAttribute,isNew,getAttributeNames etc.
 * <p/>
 * This test is classified as "quick" because we start jetty server only once.
 *
 * @since 3.3
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class WebFilterBasicTest extends AbstractWebFilterTest {

    public WebFilterBasicTest() {
        super("node1-node.xml", "node2-node.xml");
    }

    @Test(timeout = 20000)
    public void test_setAttribute() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
    }

    @Test(timeout = 20000)
    public void test_getAttribute() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        assertEquals("value", executeRequest("readIfExist", serverPort2, cookieStore));
    }

    @Test(timeout = 20000)
    public void test_getAttributeNames_WhenSessionEmpty() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("", executeRequest("names", serverPort1, cookieStore));
    }

    @Test(timeout = 20000)
    public void test_getAttributeNames_WhenSessionNotEmpty() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);

        assertEquals("key", executeRequest("names", serverPort1, cookieStore));
    }

    @Test(timeout = 20000)
    public void test_removeAttribute() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);
        executeRequest("remove", serverPort2, cookieStore);

        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
    }

    @Test(timeout = 20000)
    public void test_clusterMapSize() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        executeRequest("write", serverPort1, cookieStore);

        assertEquals(2, map.size());
    }

    @Test(timeout = 20000)
    public void test_clusterMapSizeAfterRemove() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);

        executeRequest("write", serverPort1, cookieStore);
        executeRequest("remove", serverPort2, cookieStore);

        assertEquals(1, map.size());
    }

    @Test(timeout = 20000)
    public void test_updateAttribute() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();

        executeRequest("write", serverPort1, cookieStore);
        executeRequest("update", serverPort2, cookieStore);

        assertEquals("value-updated", executeRequest("read", serverPort1, cookieStore));
        assertSizeEventually(2, map);
    }

    @Test(timeout = 20000)
    public void test_invalidateSession() throws Exception {
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        CookieStore cookieStore = new BasicCookieStore();

        executeRequest("write", serverPort1, cookieStore);
        executeRequest("invalidate", serverPort2, cookieStore);

        assertSizeEventually(0, map);
    }

    @Test(timeout = 20000)
    public void test_isNew() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();

        assertEquals("true", executeRequest("isNew", serverPort1, cookieStore));
        assertEquals("false", executeRequest("isNew", serverPort1, cookieStore));
    }

    @Test(timeout = 20000)
    public void test_sessionTimeout() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);

        executeRequest("write", serverPort1, cookieStore);
        executeRequest("timeout", serverPort1, cookieStore);
        assertSizeEventually(0, map);
    }

    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception {
        return new JettyServer(port, sourceDir, serverXml);
    }
}
