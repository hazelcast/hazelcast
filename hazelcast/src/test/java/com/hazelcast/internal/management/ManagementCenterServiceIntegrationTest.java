/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.client.impl.ClientImpl;
import com.hazelcast.config.Config;
import com.hazelcast.client.Client;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.ParseException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ManagementCenterServiceIntegrationTest {

    private static final String clusterName = "Session Integration (AWS discovery)";

    private static MancenterMock mancenterMock;
    private static HazelcastInstanceImpl instance;

    @BeforeClass
    public static void beforeClass() throws Exception {
        HazelcastInstanceFactory.terminateAll();
        mancenterMock = new MancenterMock(availablePort());
        HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) Hazelcast.newHazelcastInstance(getManagementCenterConfig());
        instance = proxy.getOriginal();
    }

    @AfterClass
    public static void tearDownClass() {
        HazelcastInstanceFactory.terminateAll();
        mancenterMock.stop();
    }

    @After
    public void tearDown() {
        mancenterMock.resetClientBwList();
    }

    @Test
    public void testTimedMemberStateNotNull() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String responseString = doHttpGet("/mancen/memberStateCheck");
                assertNotNull(responseString);
                assertNotEquals("", responseString);

                JsonObject object;
                try {
                    object = Json.parse(responseString).asObject();
                } catch (ParseException e) {
                    throw new AssertionError("Failed to parse JSON: " + responseString);
                }
                TimedMemberState memberState = new TimedMemberState();
                memberState.fromJson(object);
                assertEquals(clusterName, memberState.getClusterName());
            }
        });
    }

    @Test
    public void testGetTaskUrlEncodes() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String responseString = doHttpGet("/mancen/getClusterName");
                assertEquals(clusterName, responseString);
            }
        });
    }

    @Test
    public void testClientBwListApplies() {
        mancenterMock.enableClientWhitelist("127.0.0.1");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String responseString = doHttpGet("/mancen/memberStateCheck");
                assertNotNull(responseString);
                assertNotEquals("", responseString);

                String name = randomString();
                Set<String> labels = new HashSet<String>();
                Client client1 = new ClientImpl(null, createInetSocketAddress("127.0.0.1"), name, labels);
                assertTrue(instance.node.clientEngine.isClientAllowed(client1));

                Client client2 = new ClientImpl(null, createInetSocketAddress("127.0.0.2"), name, labels);
                assertFalse(instance.node.clientEngine.isClientAllowed(client2));
            }
        });
    }

    @Test
    public void testMemberStateNameProvided() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String responseString = doHttpGet("/mancen/memberStateCheck");
                assertNotNull(responseString);
                assertNotEquals("", responseString);

                JsonObject object;
                try {
                    object = Json.parse(responseString).asObject();
                } catch (ParseException e) {
                    throw new AssertionError("Failed to parse JSON: " + responseString);
                }
                TimedMemberState memberState = new TimedMemberState();
                memberState.fromJson(object);
                assertEquals(instance.getName(), memberState.getMemberState().getName());
            }
        });
    }

    private InetSocketAddress createInetSocketAddress(String name) {
        try {
            return new InetSocketAddress(InetAddress.getByName(name), 5000);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static int availablePort() {
        while (true) {
            int port = 8000 + (int) (1000 * Math.random());
            try {
                ServerSocket socket = new ServerSocket(port);
                socket.close();
                return port;
            } catch (Exception ignore) {
                // try next port
            }
        }
    }

    private String doHttpGet(String path) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().disableRedirectHandling().build();
        try {
            HttpUriRequest request = new HttpGet("http://localhost:" + mancenterMock.getListeningPort() + path);
            HttpResponse response = client.execute(request);
            HttpEntity entity = response.getEntity();
            return EntityUtils.toString(entity);
        } finally {
            client.close();
        }
    }

    private static Config getManagementCenterConfig() {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl(format("http://localhost:%d%s/", mancenterMock.getListeningPort(), "/mancen"));
        return config;
    }
}
