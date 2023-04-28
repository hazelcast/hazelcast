/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastCloudDiscoveryTest extends ClientTestSupport {

    private static final String RESPONSE = "["
            + " {\"private-address\":\"10.47.0.8\",\"public-address\":\"54.213.63.142:32298\"},\n"
            + " {\"private-address\":\"10.47.0.9\",\"public-address\":\"54.245.77.185:32298\"},\n"
            + " {\"private-address\":\"10.47.0.10\",\"public-address\":\"54.186.232.37:32298\"}\n"
            + "]";

    private static final String TPC_ENABLED_RESPONSE
            = "[{\"private-address\":\"10.47.0.8:30000\",\"public-address\":\"54.213.63.142:32298\",\"tpc-ports\":["
            + "{\"private-port\":40000,\"public-port\":42298},"
            + "{\"private-port\":40001,\"public-port\":42299}]},"
            + "{\"private-address\":\"10.47.0.9:30000\",\"public-address\":\"54.245.77.185:32298\",\"tpc-ports\":["
            + "{\"private-port\":32000,\"public-port\":40250},"
            + "{\"private-port\":32001,\"public-port\":40251}]}]";

    private static final String NOT_FOUND_RESPONSE = "HTTP/1.1 404 Not Found\nContent-Length: 0\n\n";
    private static final String VALID_TOKEN = "validToken";
    private static final String VALID_TPC_TOKEN = "validTpcToken";

    private HttpServer httpsServer;

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            URI requestURI = t.getRequestURI();
            if (requestURI.getPath().equals("/cluster/discovery")) {
                String[] split = requestURI.getQuery().split("=");
                if ("token".equals(split[0])) {
                    String response;
                    if (VALID_TOKEN.equals(split[1])) {
                        response = RESPONSE;
                    } else if (VALID_TPC_TOKEN.equals(split[1])) {
                        response = TPC_ENABLED_RESPONSE;
                    } else {
                        throw new IllegalStateException("Unexpected token");
                    }

                    t.sendResponseHeaders(200, response.getBytes().length);
                    OutputStream os = t.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                    return;
                }
            }

            t.sendResponseHeaders(404, NOT_FOUND_RESPONSE.getBytes().length);
            OutputStream os = t.getResponseBody();
            os.write(NOT_FOUND_RESPONSE.getBytes());
            os.close();
        }
    }

    @Before
    public void setUp() throws IOException {
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
    public void testWithValidToken() throws UnknownHostException {
        String cloudBaseUrl = "http://127.0.0.1:" + httpsServer.getAddress().getPort();
        String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudBaseUrl, VALID_TOKEN);
        HazelcastCloudDiscovery discovery = new HazelcastCloudDiscovery(urlEndpoint, Integer.MAX_VALUE, false);
        HazelcastCloudDiscovery.DiscoveryResponse response = discovery.discoverNodes();
        Map<Address, Address> privateToPublic = response.getPrivateToPublicAddresses();
        List<Address> members = response.getPrivateMemberAddresses();

        Map<Address, Address> expectedPrivateToPublic = new HashMap<>();
        expectedPrivateToPublic.put(new Address("10.47.0.8", 32298), new Address("54.213.63.142", 32298));
        expectedPrivateToPublic.put(new Address("10.47.0.9", 32298), new Address("54.245.77.185", 32298));
        expectedPrivateToPublic.put(new Address("10.47.0.10", 32298), new Address("54.186.232.37", 32298));

        assertEquals(expectedPrivateToPublic.size(), privateToPublic.size());
        for (Map.Entry<Address, Address> entry : privateToPublic.entrySet()) {
            assertEquals(expectedPrivateToPublic.get(entry.getKey()), entry.getValue());
        }

        Set<Address> expectedMembers = expectedPrivateToPublic.keySet();
        assertEquals(expectedMembers.size(), members.size());
        for (Address address : members) {
            assertTrue(expectedMembers.contains(address));
        }
    }

    @Test
    public void testWithValidTpcToken() throws UnknownHostException {
        String cloudBaseUrl = "http://127.0.0.1:" + httpsServer.getAddress().getPort();
        String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudBaseUrl, VALID_TPC_TOKEN);

        HazelcastCloudDiscovery cloudDiscovery = new HazelcastCloudDiscovery(urlEndpoint, Integer.MAX_VALUE, true);
        HazelcastCloudDiscovery.DiscoveryResponse response = cloudDiscovery.discoverNodes();
        Map<Address, Address> privateToPublic = response.getPrivateToPublicAddresses();
        List<Address> members = response.getPrivateMemberAddresses();

        Map<Address, Address> expectedPrivateToPublic = new HashMap<>();
        expectedPrivateToPublic.put(new Address("10.47.0.8", 30000), new Address("54.213.63.142", 32298));
        expectedPrivateToPublic.put(new Address("10.47.0.8", 40000), new Address("54.213.63.142", 42298));
        expectedPrivateToPublic.put(new Address("10.47.0.8", 40001), new Address("54.213.63.142", 42299));
        expectedPrivateToPublic.put(new Address("10.47.0.9", 30000), new Address("54.245.77.185", 32298));
        expectedPrivateToPublic.put(new Address("10.47.0.9", 32000), new Address("54.245.77.185", 40250));
        expectedPrivateToPublic.put(new Address("10.47.0.9", 32001), new Address("54.245.77.185", 40251));

        assertEquals(expectedPrivateToPublic.size(), privateToPublic.size());
        for (Map.Entry<Address, Address> entry : privateToPublic.entrySet()) {
            assertEquals(expectedPrivateToPublic.get(entry.getKey()), entry.getValue());
        }

        Set<Address> expectedMembers = new HashSet<>();
        expectedMembers.add(new Address("10.47.0.8", 30000));
        expectedMembers.add(new Address("10.47.0.9", 30000));

        assertEquals(expectedMembers.size(), members.size());
        for (Address address : members) {
            assertTrue(expectedMembers.contains(address));
        }
    }

    @Test(expected = HazelcastException.class)
    public void testWithInvalidToken() {
        String cloudBaseUrl = "http://127.0.0.1:" + httpsServer.getAddress().getPort();
        String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudBaseUrl, "invalid");
        HazelcastCloudDiscovery cloudDiscovery = new HazelcastCloudDiscovery(urlEndpoint, Integer.MAX_VALUE, false);
        cloudDiscovery.discoverNodes();
    }

    @Test
    public void testJsonResponseParse_withDifferentPortOnPrivateAddress() throws IOException {
        JsonValue jsonResponse = Json.parse(
                " [{\"private-address\":\"100.96.5.1:5701\",\"public-address\":\"10.113.44.139:31115\"},"
                        + "{\"private-address\":\"100.96.4.2:5701\",\"public-address\":\"10.113.44.130:31115\"} ]");
        HazelcastCloudDiscovery.DiscoveryResponse response = HazelcastCloudDiscovery.parseJsonResponse(jsonResponse, false);
        Map<Address, Address> privateToPublic = response.getPrivateToPublicAddresses();

        assertEquals(2, privateToPublic.size());
        assertEquals(new Address("10.113.44.139", 31115), privateToPublic.get(new Address("100.96.5.1", 5701)));
        assertEquals(new Address("10.113.44.130", 31115), privateToPublic.get(new Address("100.96.4.2", 5701)));

        List<Address> members = response.getPrivateMemberAddresses();

        assertEquals(2, members.size());
        assertContains(members, new Address("100.96.5.1", 5701));
        assertContains(members, new Address("100.96.4.2", 5701));
    }

    @Test
    public void testJsonResponseParse() throws IOException {
        JsonValue jsonResponse = Json.parse(
                "[{\"private-address\":\"100.96.5.1\",\"public-address\":\"10.113.44.139:31115\"},"
                        + "{\"private-address\":\"100.96.4.2\",\"public-address\":\"10.113.44.130:31115\"} ]");
        HazelcastCloudDiscovery.DiscoveryResponse response = HazelcastCloudDiscovery.parseJsonResponse(jsonResponse, false);
        Map<Address, Address> privateToPublic = response.getPrivateToPublicAddresses();

        assertEquals(2, privateToPublic.size());
        assertEquals(new Address("10.113.44.139", 31115), privateToPublic.get(new Address("100.96.5.1", 31115)));
        assertEquals(new Address("10.113.44.130", 31115), privateToPublic.get(new Address("100.96.4.2", 31115)));

        List<Address> members = response.getPrivateMemberAddresses();

        assertEquals(2, members.size());
        assertContains(members, new Address("100.96.5.1", 31115));
        assertContains(members, new Address("100.96.4.2", 31115));
    }

    @Test
    public void tesJsonResponseParse_withTpc() throws IOException {
        JsonValue jsonResponse = Json.parse(
                "[{\"private-address\":\"10.96.5.1:30000\",\"public-address\":\"100.113.44.139:31115\",\"tpc-ports\":"
                        + "[{\"private-port\":40000,\"public-port\":32115}]}]");
        HazelcastCloudDiscovery.DiscoveryResponse response = HazelcastCloudDiscovery.parseJsonResponse(jsonResponse, true);
        Map<Address, Address> privateToPublic = response.getPrivateToPublicAddresses();

        assertEquals(2, privateToPublic.size());
        assertEquals(new Address("100.113.44.139", 31115), privateToPublic.get(new Address("10.96.5.1", 30000)));
        assertEquals(new Address("100.113.44.139", 32115), privateToPublic.get(new Address("10.96.5.1", 40000)));

        List<Address> members = response.getPrivateMemberAddresses();

        assertEquals(1, members.size());
        assertContains(members, new Address("10.96.5.1", 30000));
    }

    @Test
    public void tesJsonResponseParse_withTpc_whenTpcIsDisabled() throws IOException {
        JsonValue jsonResponse = Json.parse(
                "[{\"private-address\":\"10.96.5.1:30000\",\"public-address\":\"100.113.44.139:31115\",\"tpc-ports\":"
                        + "[{\"private-port\":40000,\"public-port\":32115}]}]");
        HazelcastCloudDiscovery.DiscoveryResponse response = HazelcastCloudDiscovery.parseJsonResponse(jsonResponse, false);
        Map<Address, Address> privateToPublic = response.getPrivateToPublicAddresses();

        assertEquals(1, privateToPublic.size());
        assertEquals(new Address("100.113.44.139", 31115), privateToPublic.get(new Address("10.96.5.1", 30000)));

        List<Address> members = response.getPrivateMemberAddresses();

        assertEquals(1, members.size());
        assertContains(members, new Address("10.96.5.1", 30000));
    }
}
