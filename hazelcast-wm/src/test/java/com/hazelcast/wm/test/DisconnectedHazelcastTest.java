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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Tests to basic session methods. getAttribute,setAttribute,isNew,getAttributeNames etc.
 * <p/>
 * This test is classified as "quick" because we start jetty server only once.
 *
 * @since 3.3
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DisconnectedHazelcastTest extends AbstractWebFilterTest {

    public DisconnectedHazelcastTest() {
        super("node1-client.xml", "node2-client.xml");
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
    }

    @Override
    protected void ensureInstanceIsUp() throws Exception {
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

    @Test(timeout = 60000)
    public void test_setAttribute() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        executeRequest("write", serverPort1, cookieStore);
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("null", executeRequest("read", serverPort2, cookieStore));
    }

    @Test(timeout = 60000)
    public void test_getAttribute() throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        assertEquals("null", executeRequest("read", serverPort1, cookieStore));
        executeRequest("write", serverPort1, cookieStore);
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("value", executeRequest("readIfExist", serverPort1, cookieStore));
        assertEquals("null", executeRequest("read", serverPort2, cookieStore));
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        Thread.sleep(9000);
        executeRequest("write", serverPort1, cookieStore);
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));
    }

    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception {
        return new JettyServer(port, sourceDir, serverXml);
    }
}
