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
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.Json;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestInstanceTest {

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config createConfig() {
        return new Config();
    }

    protected Config createConfigWithRestEnabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableGroups(RestEndpointGroup.CLUSTER_READ);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected Config createConfigWithRestEnabledAndClusterReadDisabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true)
                                                         .disableGroups(RestEndpointGroup.CLUSTER_READ)
                                                         .enableGroups(RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    @Test
    public void testDisabledRest() {
        // REST should be disabled by default
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getInstanceInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Test
    public void testEnabledRestButDisabledGroupClusterRead() {
        // REST should be disabled by default
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabledAndClusterReadDisabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getInstanceInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Test
    public void testGetInstanceName()
            throws IOException {
        final HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String expected = instance.getName();
        final String response = communicator.getInstanceInfo();
        final String nameFromResponse = Json.parse(response).asObject().get("name").asString();

        assertEquals(expected, nameFromResponse);
    }

    @Test
    public void testHeadRequestToInstance()
            throws IOException {
        final HTTPCommunicator communicator = new HTTPCommunicator(factory.newHazelcastInstance(createConfigWithRestEnabled()));

        HTTPCommunicator.ConnectionResponse connectionResponse = communicator.headRequestToInstanceURI();
        assertEquals(200, connectionResponse.responseCode);
    }

}
