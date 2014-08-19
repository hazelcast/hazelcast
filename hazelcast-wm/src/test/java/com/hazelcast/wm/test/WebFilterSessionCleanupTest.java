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
import com.hazelcast.test.annotation.SlowTest;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests to verify that sessions are correctly removed from the map when timed out by multiple nodes.
 * <p/>
 * This test is classified as "slow" because the "fastest" session expiration supported by the servlet spec is still
 * 1 minute. That means this test needs to run for close to two minutes to verify cleanup.
 *
 * @since 3.3
 */
@RunWith(WebTestRunner.class)
@DelegatedRunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class WebFilterSessionCleanupTest extends AbstractWebFilterTest {

    public WebFilterSessionCleanupTest() {
        super("session-cleanup.xml", "session-cleanup.xml");
    }

    @Test(timeout = 130000)
    public void testSessionTimeout() throws Exception {
        IMap<String, Object> map = hz.getMap("default");
        CookieStore cookieStore = new BasicCookieStore();

        // Write a value into the session on one server
        assertEquals("true", executeRequest("write", serverPort1, cookieStore));

        // Find the session in the map and verify that it has one reference
        String sessionId = findHazelcastSessionId(map);
        assertEquals(2, map.size());
        assertEquals("Writing to the session should have initialized the reference count to 1",
                1, ((Integer) map.get(sessionId)).intValue());

        // We want the session lifecycles between the two servers to be offset somewhat, so wait
        // briefly and then read the session state on the second server
        Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
        assertEquals("value", executeRequest("read", serverPort2, cookieStore));

        // At this point the session should have two references, one from each server
        assertEquals(2, map.size());
        assertEquals("Accessing the same session from a different node should have incremented the reference count to 2",
                2, ((Integer) map.get(sessionId)).intValue());

        // Now we need to wait for the session to timeout. The timeout should be staggered, so
        // after another 30-45 seconds the session should be back to 1 reference
        Thread.sleep(TimeUnit.SECONDS.toMillis(45));
        assertEquals(2, map.size());
        assertEquals("Session timeout on one node should have reduced the reference count to 1",
                1, ((Integer) map.get(sessionId)).intValue());

        // Wait for the session to timeout on the other server, at which point it should be
        // fully removed from the map
        Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
        assertTrue("Session timeout on both nodes should have removed the IMap entries", map.isEmpty());
    }

    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception {
        return new JettyServer(port,sourceDir,serverXml);
    }
}
