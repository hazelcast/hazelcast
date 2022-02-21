/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.ascii;

import static com.hazelcast.test.Accessors.getAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests enabling HTTP REST API by {@link RestApiConfig}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestApiFilterTest extends RestApiConfigTestBase {

    /**
     * <pre>
     * Given: RestApiConfig is explicitly disabled
     * When: a HTTP GET method is used by client
     * Then: connection is terminated after reading the first 3 bytes (protocol header)
     * </pre>
     */
    @Test
    public void testRestApiDisabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig().setEnabled(false));
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData(GET);
            client.waitUntilClosed();
            assertEquals(3, client.getSentBytesCount());
            assertEquals(0, client.getReceivedBytes().length);
            assertTrue(client.isConnectionClosed());
        } finally {
            client.close();
        }
    }

    /**
     * <pre>
     * Given: RestApiConfig is not provided (default is used)
     * When: a HTTP GET method is used by client
     * Then: connection is terminated after reading the first 3 bytes (protocol header)
     * </pre>
     */
    @Test
    public void testRestApiDisabledByDefault() throws Exception {
        HazelcastInstance hz = factory.newHazelcastInstance(null);
        TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData(GET);
            client.waitUntilClosed();
            assertEquals(3, client.getSentBytesCount());
            assertEquals(0, client.getReceivedBytes().length);
            assertTrue(client.isConnectionClosed());
        } finally {
            client.close();
        }
    }

    /**
     * <pre>
     * Given: RestApiConfig is explicitly enabled
     * When: a memcache command prefix is used by client
     * Then: connection is terminated after reading the first 3 bytes (protocol header)
     * </pre>
     */
    @Test
    public void testMemcacheWhenRestApiEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig().setEnabled(true));
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData("ver");
            client.waitUntilClosed();
            assertEquals(3, client.getSentBytesCount());
            assertEquals(0, client.getReceivedBytes().length);
            assertTrue(client.isConnectionClosed());
        } finally {
            client.close();
        }
    }

    /**
     * <pre>
     * Given: RestApiConfig is explicitly enabled
     * When: HTTP GET command is used to retrieve a non-Hazelcast resource
     * Then: connection is terminated after reading the command line
     * </pre>
     */
    @Test
    public void testHttpGetForUnknownResource() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig().setEnabled(true));
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData(GET);
            client.waitUntilClosed(2000);
            assertFalse(client.isConnectionClosed());
            client.sendData(" /test/abc HTTP/1.0" + CRLF);
            client.waitUntilClosed();
            assertEquals(0, client.getReceivedBytes().length);
            assertTrue(client.isConnectionClosed());
        } finally {
            client.close();
        }
    }
}
