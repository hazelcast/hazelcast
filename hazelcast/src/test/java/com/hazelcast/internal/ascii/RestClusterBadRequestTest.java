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

package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.HTTPCommunicator.ConnectionResponse;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_ADD_WAN_CONFIG;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_CHANGE_CLUSTER_STATE_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_CLUSTER_NODES_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_CLUSTER_STATE_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_CLUSTER_VERSION_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_CP_GROUPS_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_CP_MEMBERS_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_FORCESTART_CLUSTER_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_HOT_RESTART_BACKUP_CLUSTER_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_LOCAL_CP_MEMBER_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_PARTIALSTART_CLUSTER_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_RESET_CP_SUBSYSTEM_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_SHUTDOWN_CLUSTER_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_SHUTDOWN_NODE_CLUSTER_URL;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_WAN_CLEAR_QUEUES;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_WAN_CONSISTENCY_CHECK_MAP;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_WAN_PAUSE_PUBLISHER;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_WAN_RESUME_PUBLISHER;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_WAN_STOP_PUBLISHER;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_WAN_SYNC_ALL_MAPS;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_WAN_SYNC_MAP;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestClusterBadRequestTest extends HazelcastTestSupport {

    private static final List<String> POST_AUTHENTICATED_URIS = Arrays.asList(
            URI_CLUSTER_STATE_URL,
            URI_CHANGE_CLUSTER_STATE_URL,
            URI_CLUSTER_VERSION_URL,
            URI_SHUTDOWN_CLUSTER_URL,
            URI_SHUTDOWN_NODE_CLUSTER_URL,
            URI_CLUSTER_NODES_URL,
            URI_FORCESTART_CLUSTER_URL,
            URI_PARTIALSTART_CLUSTER_URL,
            URI_HOT_RESTART_BACKUP_CLUSTER_URL,
            URI_HOT_RESTART_BACKUP_INTERRUPT_CLUSTER_URL,
            URI_WAN_SYNC_MAP,
            URI_WAN_SYNC_ALL_MAPS,
            URI_WAN_CLEAR_QUEUES,
            URI_ADD_WAN_CONFIG,
            URI_WAN_PAUSE_PUBLISHER,
            URI_WAN_STOP_PUBLISHER,
            URI_WAN_RESUME_PUBLISHER,
            URI_WAN_CONSISTENCY_CHECK_MAP,
            URI_RESET_CP_SUBSYSTEM_URL,
            URI_CP_GROUPS_URL,
            URI_CP_MEMBERS_URL,
            URI_LOCAL_CP_MEMBER_URL);

    private TestAwareInstanceFactory factory;
    private HTTPCommunicator communicator;

    @Before
    public void setup() {
        factory = new TestAwareInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());
        communicator = new HTTPCommunicator(instance);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableAllGroups();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                  .setEnabled(false);
        joinConfig.getTcpIpConfig()
                  .setEnabled(true)
                  .addMember("127.0.0.1");
        return config;
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testBadRequest() throws IOException {
        for (String uri : POST_AUTHENTICATED_URIS) {
            ConnectionResponse response = communicator.doPost(communicator.getUrl(uri), "dummy");
            assertEquals(uri, HttpURLConnection.HTTP_BAD_REQUEST, response.responseCode);
            assertJsonContains(uri, response.response, "status", "fail");
        }
    }

    @Test
    public void testForbidden() throws IOException {
        for (String uri : POST_AUTHENTICATED_URIS) {
            ConnectionResponse response = communicator.doPost(communicator.getUrl(uri), "dummy", "", "", "", "");
            assertEquals(uri, HttpURLConnection.HTTP_FORBIDDEN, response.responseCode);
            assertJsonContains(uri, response.response, "status", "fail", "message", "unauthenticated");
        }
    }


    private JsonObject assertJsonContains(String uri, String json, String... attributesAndValues) {
        JsonObject object = Json.parse(json).asObject();
        for (int i = 0; i < attributesAndValues.length; ) {
            String key = attributesAndValues[i++];
            String expectedValue = attributesAndValues[i++];
            assertEquals(uri, expectedValue, object.getString(key, null));
        }
        return object;
    }
}
