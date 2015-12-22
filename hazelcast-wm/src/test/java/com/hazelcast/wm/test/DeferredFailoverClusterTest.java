/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.web.SessionState;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests to basic session methods. getAttribute,setAttribute,isNew,getAttributeNames etc.
 * <p/>
 * This test is classified as "quick" because we start jetty server only once.
 *
 * @since 3.3
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DeferredFailoverClusterTest extends AbstractWebFilterTest {

    public DeferredFailoverClusterTest() {
        super("node1-client-deferred.xml", "node2-client-deferred.xml");
    }

    @Before
    @Override
    public void setup() throws Exception {
        Hazelcast.shutdownAll();
        ContainerContext cc = CONTAINER_CONTEXT_MAP.get(getClass());
        // If container is not exist yet or
        // Hazelcast instance is not active (because of such as server shutdown)
        if (cc == null) {
            // Build a new instance
            ensureInstanceIsUp();
            CONTAINER_CONTEXT_MAP.put(
                    getClass(),
                    new ContainerContext(
                            this,
                            serverXml1,
                            serverXml2,
                            serverPort1,
                            serverPort2,
                            server1,
                            server2,
                            null));
        } else {
            // For every test method a different test class can be constructed for parallel runs by JUnit.
            // So container can be exist, but configurations of current test may not be exist.
            // For this reason, we should copy container context information (such as ports, servers, ...)
            // to current test.
            cc.copyInto(this);
            // Ensure that instance is up and running
            ensureInstanceIsUp();
            // After ensuring that system is up, new containers or instance may be created.
            // So, we should copy current information from test to current context.
            cc.copyFrom(this);
        }
        // Clear map
//        IMap<String, Object> map = hz.getMap(DEFAULT_MAP_NAME);
//        map.clear();
    }

    @Override
    protected void ensureInstanceIsUp() throws Exception {
        if (isInstanceNotActive(hz)) {
            hz = Hazelcast.newHazelcastInstance(
                    new FileSystemXmlConfig(new File(sourceDir + "/WEB-INF/", "hazelcast.xml")));
        }
        if (serverXml1 != null) {
            if (server1 == null) {
                serverPort1 = availablePort();
                server1 = getServletContainer(serverPort1, sourceDir, serverXml1);
            } else if (!server1.isRunning()) {
                server1.start();
            }
        }
        if (serverXml2 != null) {
            if (server2 == null) {
                serverPort2 = availablePort();
                server2 = getServletContainer(serverPort2, sourceDir, serverXml2);
            } else if (!server2.isRunning()) {
                server2.start();
            }
        }
    }

    @Test(timeout = 20000)
    public void test_setAttribute() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("setAttribute?key=value&key2=value2&key2=value22", serverPort1, cookieStore);
        IMap<Object, Object> map = hz.getMap(DEFAULT_MAP_NAME);
        assertEquals(1, map.size());
        String hazelcastSessionId = getHazelcastSessionId(cookieStore);
        SessionState sessionState = (SessionState) map.get(hazelcastSessionId);
        assertEquals(2, sessionState.getAttributes().size());
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        assertEquals(1, map.size());
        server1.stop();
        assertEquals(1, map.size());
        sessionState = (SessionState) map.get(hazelcastSessionId);
        assertEquals(2, sessionState.getAttributes().size());
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        server2.stop();
        assertEquals(1, map.size());
        sessionState = (SessionState) map.get(hazelcastSessionId);
        assertNotNull(sessionState);
        server1.start();
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));
        server2.start();
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
        executeRequest("invalidate", serverPort2, cookieStore);
        assertEquals(0, map.size());
        assertNull(map.get(hazelcastSessionId));
        assertEquals("null", executeRequest("read", serverPort2, cookieStore));
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
    }

    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception {
        return new JettyServer(port, sourceDir, serverXml);
    }
}
