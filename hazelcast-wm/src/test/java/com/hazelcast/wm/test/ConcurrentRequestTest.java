/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ConcurrentRequestTest extends AbstractWebFilterTest {

    public ConcurrentRequestTest() {
        super("node1-node-deferred.xml", "node2-node-deferred.xml");
    }


    @Test(timeout = 60000)
    public void test_multipleRequest() throws Exception {
        final CookieStore cookieStore = new BasicCookieStore();
        executeRequest("read", serverPort1, cookieStore);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    executeRequest("write_wait", serverPort1, cookieStore);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        Thread.sleep(500);
        executeRequest("read", serverPort1, cookieStore);
        thread.join();
        assertEquals("value", executeRequest("read", serverPort1, cookieStore));
    }


    @Override
    protected ServletContainer getServletContainer(int port, String sourceDir, String serverXml) throws Exception {
        return new JettyServer(port, sourceDir, serverXml);
    }
}
