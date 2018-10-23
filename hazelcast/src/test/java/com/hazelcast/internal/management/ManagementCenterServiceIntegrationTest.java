/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.ParseException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ManagementCenterServiceIntegrationTest {

    private static final String clusterName = "Session Integration (AWS discovery)";

    private static MancenterMock mancenterMock;

    @BeforeClass
    public static void beforeClass() throws Exception {
        HazelcastInstanceFactory.terminateAll();
        mancenterMock = new MancenterMock(availablePort());
        Hazelcast.newHazelcastInstance(getManagementCenterConfig());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HazelcastInstanceFactory.terminateAll();
        mancenterMock.stop();
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
        config.getGroupConfig().setName(clusterName);
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl(format("http://localhost:%d%s/", mancenterMock.getListeningPort(), "/mancen"));
        return config;
    }
}
