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

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastCloudDiscoveryTest extends ClientTestSupport {

    private static final String response = "["
            + " {\"private-address\":\"10.47.0.8\",\"public-address\":\"54.213.63.142:32298\"},\n"
            + " {\"private-address\":\"10.47.0.9\",\"public-address\":\"54.245.77.185:32298\"},\n"
            + " {\"private-address\":\"10.47.0.10\",\"public-address\":\"54.186.232.37:32298\"}\n"
            + "]";

    private static final String notFoundResponse = "HTTP/1.1 404 Not Found\nContent-Length: 0\n\n";

    private Map<Address, Address> addresses = new HashMap<Address, Address>();
    private HttpServer httpsServer;
    private static String validToken = "validToken";

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            URI requestURI = t.getRequestURI();
            if (requestURI.getPath().equals("/cluster/discovery")) {
                String[] split = requestURI.getQuery().split("=");
                if ("token".equals(split[0]) && validToken.equals(split[1])) {
                    t.sendResponseHeaders(200, response.getBytes().length);
                    OutputStream os = t.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                    return;
                }
            }

            t.sendResponseHeaders(404, notFoundResponse.getBytes().length);
            OutputStream os = t.getResponseBody();
            os.write(notFoundResponse.getBytes());
            os.close();
        }


    }

    @Before
    public void setUp() throws IOException {
        addresses.put(new Address("10.47.0.8", 32298), new Address("54.213.63.142", 32298));
        addresses.put(new Address("10.47.0.9", 32298), new Address("54.245.77.185", 32298));
        addresses.put(new Address("10.47.0.10", 32298), new Address("54.186.232.37", 32298));

        httpsServer = HttpServer.create(new InetSocketAddress(0), 0);

        httpsServer.createContext("/", new MyHandler());
        httpsServer.setExecutor(null); // creates a default executor
        httpsServer.start();
    }

    @After
    public void tearDown() {
        httpsServer.stop(0);
    }

    @Test
    public void testWithValidToken() {
        String cloudBaseUrl = "http://127.0.0.1:" + httpsServer.getAddress().getPort();
        String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudBaseUrl, validToken);
        HazelcastCloudDiscovery cloudDiscovery = new HazelcastCloudDiscovery(urlEndpoint, Integer.MAX_VALUE);
        Map<Address, Address> addressMap = cloudDiscovery.discoverNodes();

        assertEquals(addressMap.size(), addresses.size());
        for (Map.Entry<Address, Address> entry : addressMap.entrySet()) {
            assertEquals(addresses.get(entry.getKey()), entry.getValue());
        }

    }

    @Test(expected = HazelcastException.class)
    public void testWithInvalidToken() {
        String cloudBaseUrl = "http://127.0.0.1:" + httpsServer.getAddress().getPort();
        String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudBaseUrl, "invalid");
        HazelcastCloudDiscovery cloudDiscovery = new HazelcastCloudDiscovery(urlEndpoint, Integer.MAX_VALUE);
        cloudDiscovery.discoverNodes();
    }

    @Test
    public void testJsonResponseParse_withDifferentPortOnPrivateAddress() throws IOException {
        JsonValue jsonResponse = Json.parse(
                " [{\"private-address\":\"100.96.5.1:5701\",\"public-address\":\"10.113.44.139:31115\"},"
                        + "{\"private-address\":\"100.96.4.2:5701\",\"public-address\":\"10.113.44.130:31115\"} ]");
        Map<Address, Address> privatePublicMap = HazelcastCloudDiscovery.parseJsonResponse(jsonResponse);
        assertEquals(2, privatePublicMap.size());
        assertEquals(new Address("10.113.44.139", 31115), privatePublicMap.get(new Address("100.96.5.1", 5701)));
        assertEquals(new Address("10.113.44.130", 31115), privatePublicMap.get(new Address("100.96.4.2", 5701)));
    }

    @Test
    public void testJsonResponseParse() throws IOException {
        JsonValue jsonResponse = Json.parse(
                "[{\"private-address\":\"100.96.5.1\",\"public-address\":\"10.113.44.139:31115\"},"
                        + "{\"private-address\":\"100.96.4.2\",\"public-address\":\"10.113.44.130:31115\"} ]");
        Map<Address, Address> privatePublicMap = HazelcastCloudDiscovery.parseJsonResponse(jsonResponse);
        assertEquals(2, privatePublicMap.size());
        assertEquals(new Address("10.113.44.139", 31115), privatePublicMap.get(new Address("100.96.5.1", 31115)));
        assertEquals(new Address("10.113.44.130", 31115), privatePublicMap.get(new Address("100.96.4.2", 31115)));
    }

}
